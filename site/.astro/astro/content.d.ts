declare module 'astro:content' {
	interface RenderResult {
		Content: import('astro/runtime/server/index.js').AstroComponentFactory;
		headings: import('astro').MarkdownHeading[];
		remarkPluginFrontmatter: Record<string, any>;
	}
	interface Render {
		'.md': Promise<RenderResult>;
	}

	export interface RenderedContent {
		html: string;
		metadata?: {
			imagePaths: Array<string>;
			[key: string]: unknown;
		};
	}
}

declare module 'astro:content' {
	type Flatten<T> = T extends { [K: string]: infer U } ? U : never;

	export type CollectionKey = keyof AnyEntryMap;
	export type CollectionEntry<C extends CollectionKey> = Flatten<AnyEntryMap[C]>;

	export type ContentCollectionKey = keyof ContentEntryMap;
	export type DataCollectionKey = keyof DataEntryMap;

	type AllValuesOf<T> = T extends any ? T[keyof T] : never;
	type ValidContentEntrySlug<C extends keyof ContentEntryMap> = AllValuesOf<
		ContentEntryMap[C]
	>['slug'];

	/** @deprecated Use `getEntry` instead. */
	export function getEntryBySlug<
		C extends keyof ContentEntryMap,
		E extends ValidContentEntrySlug<C> | (string & {}),
	>(
		collection: C,
		// Note that this has to accept a regular string too, for SSR
		entrySlug: E,
	): E extends ValidContentEntrySlug<C>
		? Promise<CollectionEntry<C>>
		: Promise<CollectionEntry<C> | undefined>;

	/** @deprecated Use `getEntry` instead. */
	export function getDataEntryById<C extends keyof DataEntryMap, E extends keyof DataEntryMap[C]>(
		collection: C,
		entryId: E,
	): Promise<CollectionEntry<C>>;

	export function getCollection<C extends keyof AnyEntryMap, E extends CollectionEntry<C>>(
		collection: C,
		filter?: (entry: CollectionEntry<C>) => entry is E,
	): Promise<E[]>;
	export function getCollection<C extends keyof AnyEntryMap>(
		collection: C,
		filter?: (entry: CollectionEntry<C>) => unknown,
	): Promise<CollectionEntry<C>[]>;

	export function getEntry<
		C extends keyof ContentEntryMap,
		E extends ValidContentEntrySlug<C> | (string & {}),
	>(entry: {
		collection: C;
		slug: E;
	}): E extends ValidContentEntrySlug<C>
		? Promise<CollectionEntry<C>>
		: Promise<CollectionEntry<C> | undefined>;
	export function getEntry<
		C extends keyof DataEntryMap,
		E extends keyof DataEntryMap[C] | (string & {}),
	>(entry: {
		collection: C;
		id: E;
	}): E extends keyof DataEntryMap[C]
		? Promise<DataEntryMap[C][E]>
		: Promise<CollectionEntry<C> | undefined>;
	export function getEntry<
		C extends keyof ContentEntryMap,
		E extends ValidContentEntrySlug<C> | (string & {}),
	>(
		collection: C,
		slug: E,
	): E extends ValidContentEntrySlug<C>
		? Promise<CollectionEntry<C>>
		: Promise<CollectionEntry<C> | undefined>;
	export function getEntry<
		C extends keyof DataEntryMap,
		E extends keyof DataEntryMap[C] | (string & {}),
	>(
		collection: C,
		id: E,
	): E extends keyof DataEntryMap[C]
		? Promise<DataEntryMap[C][E]>
		: Promise<CollectionEntry<C> | undefined>;

	/** Resolve an array of entry references from the same collection */
	export function getEntries<C extends keyof ContentEntryMap>(
		entries: {
			collection: C;
			slug: ValidContentEntrySlug<C>;
		}[],
	): Promise<CollectionEntry<C>[]>;
	export function getEntries<C extends keyof DataEntryMap>(
		entries: {
			collection: C;
			id: keyof DataEntryMap[C];
		}[],
	): Promise<CollectionEntry<C>[]>;

	export function render<C extends keyof AnyEntryMap>(
		entry: AnyEntryMap[C][string],
	): Promise<RenderResult>;

	export function reference<C extends keyof AnyEntryMap>(
		collection: C,
	): import('astro/zod').ZodEffects<
		import('astro/zod').ZodString,
		C extends keyof ContentEntryMap
			? {
					collection: C;
					slug: ValidContentEntrySlug<C>;
				}
			: {
					collection: C;
					id: keyof DataEntryMap[C];
				}
	>;
	// Allow generic `string` to avoid excessive type errors in the config
	// if `dev` is not running to update as you edit.
	// Invalid collection names will be caught at build time.
	export function reference<C extends string>(
		collection: C,
	): import('astro/zod').ZodEffects<import('astro/zod').ZodString, never>;

	type ReturnTypeOrOriginal<T> = T extends (...args: any[]) => infer R ? R : T;
	type InferEntrySchema<C extends keyof AnyEntryMap> = import('astro/zod').infer<
		ReturnTypeOrOriginal<Required<ContentConfig['collections'][C]>['schema']>
	>;

	type ContentEntryMap = {
		"subsidies": {
"adachi/2026-05-05-solar-chikudenchi.md": {
	id: "adachi/2026-05-05-solar-chikudenchi.md";
  slug: "adachi/2026-05-05-solar-chikudenchi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"adachi/2026-05-29-taishin-kaishu.md": {
	id: "adachi/2026-05-29-taishin-kaishu.md";
  slug: "adachi/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ageo/2026-05-10-sogyo.md": {
	id: "ageo/2026-05-10-sogyo.md";
  slug: "ageo/2026-05-10-sogyo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ageo/2026-05-10-taiyoko.md": {
	id: "ageo/2026-05-10-taiyoko.md";
  slug: "ageo/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ageo/2026-05-29-taishin-kaishu.md": {
	id: "ageo/2026-05-29-taishin-kaishu.md";
  slug: "ageo/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ageo/2026-06-05-shoene-kaishu.md": {
	id: "ageo/2026-06-05-shoene-kaishu.md";
  slug: "ageo/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"agui/2026-05-10-shoene-aichi-pref.md": {
	id: "agui/2026-05-10-shoene-aichi-pref.md";
  slug: "agui/2026-05-10-shoene-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"agui/2026-05-10-sogyo-aichi-pref.md": {
	id: "agui/2026-05-10-sogyo-aichi-pref.md";
  slug: "agui/2026-05-10-sogyo-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"agui/2026-06-03-taishin-kaishu.md": {
	id: "agui/2026-06-03-taishin-kaishu.md";
  slug: "agui/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aichi_pref/2026-05-10-chusho-dx.md": {
	id: "aichi_pref/2026-05-10-chusho-dx.md";
  slug: "aichi_pref/2026-05-10-chusho-dx";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aichi_pref/2026-05-10-jutaku-shoene.md": {
	id: "aichi_pref/2026-05-10-jutaku-shoene.md";
  slug: "aichi_pref/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aichi_pref/2026-05-10-sogyo-shien.md": {
	id: "aichi_pref/2026-05-10-sogyo-shien.md";
  slug: "aichi_pref/2026-05-10-sogyo-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aichi_pref/2026-05-17-jgrants-cdyffmah.md": {
	id: "aichi_pref/2026-05-17-jgrants-cdyffmah.md";
  slug: "aichi_pref/2026-05-17-jgrants-cdyffmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aichi_pref/2026-05-17-jgrants-cdyfomax.md": {
	id: "aichi_pref/2026-05-17-jgrants-cdyfomax.md";
  slug: "aichi_pref/2026-05-17-jgrants-cdyfomax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aichi_pref/2026-05-17-jgrants-cdyvzmap.md": {
	id: "aichi_pref/2026-05-17-jgrants-cdyvzmap.md";
  slug: "aichi_pref/2026-05-17-jgrants-cdyvzmap";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aichi_pref/2026-05-17-jgrants-cdyw0map.md": {
	id: "aichi_pref/2026-05-17-jgrants-cdyw0map.md";
  slug: "aichi_pref/2026-05-17-jgrants-cdyw0map";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aichi_pref/2026-05-17-jgrants-cdyw1map.md": {
	id: "aichi_pref/2026-05-17-jgrants-cdyw1map.md";
  slug: "aichi_pref/2026-05-17-jgrants-cdyw1map";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aichi_pref/2026-05-17-jgrants-cdyw2map.md": {
	id: "aichi_pref/2026-05-17-jgrants-cdyw2map.md";
  slug: "aichi_pref/2026-05-17-jgrants-cdyw2map";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aichi_pref/2026-05-17-jgrants-cdyw3map.md": {
	id: "aichi_pref/2026-05-17-jgrants-cdyw3map.md";
  slug: "aichi_pref/2026-05-17-jgrants-cdyw3map";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aichi_pref/2026-05-17-jgrants-cdyw4map.md": {
	id: "aichi_pref/2026-05-17-jgrants-cdyw4map.md";
  slug: "aichi_pref/2026-05-17-jgrants-cdyw4map";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aichi_pref/2026-05-17-jgrants-cdyw5map.md": {
	id: "aichi_pref/2026-05-17-jgrants-cdyw5map.md";
  slug: "aichi_pref/2026-05-17-jgrants-cdyw5map";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aichi_pref/2026-05-17-jgrants-cdyw6map.md": {
	id: "aichi_pref/2026-05-17-jgrants-cdyw6map.md";
  slug: "aichi_pref/2026-05-17-jgrants-cdyw6map";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aichi_pref/2026-05-17-jgrants-cdyw7map.md": {
	id: "aichi_pref/2026-05-17-jgrants-cdyw7map.md";
  slug: "aichi_pref/2026-05-17-jgrants-cdyw7map";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aichi_pref/2026-06-03-shoene-kaishu.md": {
	id: "aichi_pref/2026-06-03-shoene-kaishu.md";
  slug: "aichi_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aichi_pref/2026-06-03-taishin-kaishu.md": {
	id: "aichi_pref/2026-06-03-taishin-kaishu.md";
  slug: "aichi_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aioi/2026-05-11-taiyoko.md": {
	id: "aioi/2026-05-11-taiyoko.md";
  slug: "aioi/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aioi/2026-05-11-teijyu.md": {
	id: "aioi/2026-05-11-teijyu.md";
  slug: "aioi/2026-05-11-teijyu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aioi/2026-05-29-taishin-kaishu.md": {
	id: "aioi/2026-05-29-taishin-kaishu.md";
  slug: "aioi/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aioi/2026-06-05-shoene-kaishu.md": {
	id: "aioi/2026-06-05-shoene-kaishu.md";
  slug: "aioi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aisai/2026-05-10-jutaku-reform.md": {
	id: "aisai/2026-05-10-jutaku-reform.md";
  slug: "aisai/2026-05-10-jutaku-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aisai/2026-05-10-jutaku-shoene.md": {
	id: "aisai/2026-05-10-jutaku-shoene.md";
  slug: "aisai/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aisai/2026-06-03-taishin-kaishu.md": {
	id: "aisai/2026-06-03-taishin-kaishu.md";
  slug: "aisai/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aisho/2026-05-12-taiyoko.md": {
	id: "aisho/2026-05-12-taiyoko.md";
  slug: "aisho/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aisho/2026-05-29-taishin-kaishu.md": {
	id: "aisho/2026-05-29-taishin-kaishu.md";
  slug: "aisho/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aisho/2026-06-05-shoene-kaishu.md": {
	id: "aisho/2026-06-05-shoene-kaishu.md";
  slug: "aisho/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aizuwakamatsu/2026-05-29-taishin-kaishu.md": {
	id: "aizuwakamatsu/2026-05-29-taishin-kaishu.md";
  slug: "aizuwakamatsu/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aizuwakamatsu/2026-06-05-shoene-kaishu.md": {
	id: "aizuwakamatsu/2026-06-05-shoene-kaishu.md";
  slug: "aizuwakamatsu/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"akashi/2026-05-11-kosodatechiiki.md": {
	id: "akashi/2026-05-11-kosodatechiiki.md";
  slug: "akashi/2026-05-11-kosodatechiiki";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"akashi/2026-05-11-taiyoko.md": {
	id: "akashi/2026-05-11-taiyoko.md";
  slug: "akashi/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"akashi/2026-05-29-taishin-kaishu.md": {
	id: "akashi/2026-05-29-taishin-kaishu.md";
  slug: "akashi/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"akashi/2026-06-05-shoene-kaishu.md": {
	id: "akashi/2026-06-05-shoene-kaishu.md";
  slug: "akashi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"akiruno/2026-05-12-iju.md": {
	id: "akiruno/2026-05-12-iju.md";
  slug: "akiruno/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"akiruno/2026-05-12-taiyoko.md": {
	id: "akiruno/2026-05-12-taiyoko.md";
  slug: "akiruno/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"akiruno/2026-06-03-taishin-kaishu.md": {
	id: "akiruno/2026-06-03-taishin-kaishu.md";
  slug: "akiruno/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"akiruno/2026-06-05-shoene-kaishu.md": {
	id: "akiruno/2026-06-05-shoene-kaishu.md";
  slug: "akiruno/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"akishima/2026-05-12-taiyoko.md": {
	id: "akishima/2026-05-12-taiyoko.md";
  slug: "akishima/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"akishima/2026-06-03-taishin-kaishu.md": {
	id: "akishima/2026-06-03-taishin-kaishu.md";
  slug: "akishima/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"akishima/2026-06-05-shoene-kaishu.md": {
	id: "akishima/2026-06-05-shoene-kaishu.md";
  slug: "akishima/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"akita/2026-05-24-chusho-yushi.md": {
	id: "akita/2026-05-24-chusho-yushi.md";
  slug: "akita/2026-05-24-chusho-yushi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"akita/2026-05-29-taishin-kaishu.md": {
	id: "akita/2026-05-29-taishin-kaishu.md";
  slug: "akita/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"akita/2026-06-05-shoene-kaden.md": {
	id: "akita/2026-06-05-shoene-kaden.md";
  slug: "akita/2026-06-05-shoene-kaden";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"akita/2026-06-05-shoene-kaishu.md": {
	id: "akita/2026-06-05-shoene-kaishu.md";
  slug: "akita/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"akita_pref/2026-05-17-jgrants-cdzd3mah.md": {
	id: "akita_pref/2026-05-17-jgrants-cdzd3mah.md";
  slug: "akita_pref/2026-05-17-jgrants-cdzd3mah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"akita_pref/2026-06-03-shoene-kaishu.md": {
	id: "akita_pref/2026-06-03-shoene-kaishu.md";
  slug: "akita_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"akita_pref/2026-06-03-taishin-kaishu.md": {
	id: "akita_pref/2026-06-03-taishin-kaishu.md";
  slug: "akita_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ako/2026-05-11-akiya.md": {
	id: "ako/2026-05-11-akiya.md";
  slug: "ako/2026-05-11-akiya";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ako/2026-05-11-taiyoko.md": {
	id: "ako/2026-05-11-taiyoko.md";
  slug: "ako/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ako/2026-05-29-taishin-kaishu.md": {
	id: "ako/2026-05-29-taishin-kaishu.md";
  slug: "ako/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ako/2026-06-05-shoene-kaishu.md": {
	id: "ako/2026-06-05-shoene-kaishu.md";
  slug: "ako/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ama/2026-05-10-jutaku-shoene.md": {
	id: "ama/2026-05-10-jutaku-shoene.md";
  slug: "ama/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ama/2026-05-10-taishin.md": {
	id: "ama/2026-05-10-taishin.md";
  slug: "ama/2026-05-10-taishin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"amagasaki/2026-05-11-sogyo.md": {
	id: "amagasaki/2026-05-11-sogyo.md";
  slug: "amagasaki/2026-05-11-sogyo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"amagasaki/2026-05-11-taiyoko.md": {
	id: "amagasaki/2026-05-11-taiyoko.md";
  slug: "amagasaki/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"amagasaki/2026-05-29-taishin-kaishu.md": {
	id: "amagasaki/2026-05-29-taishin-kaishu.md";
  slug: "amagasaki/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"amagasaki/2026-06-05-shoene-kaishu.md": {
	id: "amagasaki/2026-06-05-shoene-kaishu.md";
  slug: "amagasaki/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"anjo/2026-05-10-jutaku-shoene.md": {
	id: "anjo/2026-05-10-jutaku-shoene.md";
  slug: "anjo/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"anjo/2026-05-10-sogyo-shien.md": {
	id: "anjo/2026-05-10-sogyo-shien.md";
  slug: "anjo/2026-05-10-sogyo-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"anjo/2026-05-29-taishin-kaishu.md": {
	id: "anjo/2026-05-29-taishin-kaishu.md";
  slug: "anjo/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aomori/2026-05-23-chusho-yushi.md": {
	id: "aomori/2026-05-23-chusho-yushi.md";
  slug: "aomori/2026-05-23-chusho-yushi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aomori/2026-05-24-ijuu-shienkin.md": {
	id: "aomori/2026-05-24-ijuu-shienkin.md";
  slug: "aomori/2026-05-24-ijuu-shienkin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aomori/2026-05-29-taishin-kaishu.md": {
	id: "aomori/2026-05-29-taishin-kaishu.md";
  slug: "aomori/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aomori/2026-06-05-shoene-kaishu.md": {
	id: "aomori/2026-06-05-shoene-kaishu.md";
  slug: "aomori/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aomori_pref/2026-05-17-jgrants-cdzcymah.md": {
	id: "aomori_pref/2026-05-17-jgrants-cdzcymah.md";
  slug: "aomori_pref/2026-05-17-jgrants-cdzcymah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aomori_pref/2026-06-03-shoene-kaishu.md": {
	id: "aomori_pref/2026-06-03-shoene-kaishu.md";
  slug: "aomori_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"aomori_pref/2026-06-03-taishin-kaishu.md": {
	id: "aomori_pref/2026-06-03-taishin-kaishu.md";
  slug: "aomori_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"arakawa/2026-05-05-eco-josei.md": {
	id: "arakawa/2026-05-05-eco-josei.md";
  slug: "arakawa/2026-05-05-eco-josei";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"arakawa/2026-05-29-taishin-kaishu.md": {
	id: "arakawa/2026-05-29-taishin-kaishu.md";
  slug: "arakawa/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"arakawa/2026-06-05-shoene-kaishu.md": {
	id: "arakawa/2026-06-05-shoene-kaishu.md";
  slug: "arakawa/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"arida/2026-05-12-taiyoko.md": {
	id: "arida/2026-05-12-taiyoko.md";
  slug: "arida/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"arida/2026-06-03-taishin-kaishu.md": {
	id: "arida/2026-06-03-taishin-kaishu.md";
  slug: "arida/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"arida/2026-06-05-shoene-kaishu.md": {
	id: "arida/2026-06-05-shoene-kaishu.md";
  slug: "arida/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"asago/2026-05-11-iju.md": {
	id: "asago/2026-05-11-iju.md";
  slug: "asago/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"asago/2026-05-11-taiyoko.md": {
	id: "asago/2026-05-11-taiyoko.md";
  slug: "asago/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"asago/2026-05-29-taishin-kaishu.md": {
	id: "asago/2026-05-29-taishin-kaishu.md";
  slug: "asago/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"asago/2026-06-05-shoene-kaishu.md": {
	id: "asago/2026-06-05-shoene-kaishu.md";
  slug: "asago/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"asahikawa/2026-05-29-jutaku-kaishu.md": {
	id: "asahikawa/2026-05-29-jutaku-kaishu.md";
  slug: "asahikawa/2026-05-29-jutaku-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"asahikawa/2026-05-29-taishin-kaishu.md": {
	id: "asahikawa/2026-05-29-taishin-kaishu.md";
  slug: "asahikawa/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"asahikawa/2026-06-05-shoene-kaishu.md": {
	id: "asahikawa/2026-06-05-shoene-kaishu.md";
  slug: "asahikawa/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"asaka/2026-05-10-shoene.md": {
	id: "asaka/2026-05-10-shoene.md";
  slug: "asaka/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"asaka/2026-05-10-taiyoko.md": {
	id: "asaka/2026-05-10-taiyoko.md";
  slug: "asaka/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"asaka/2026-06-03-taishin-kaishu.md": {
	id: "asaka/2026-06-03-taishin-kaishu.md";
  slug: "asaka/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ashikaga/2026-05-29-taishin-kaishu.md": {
	id: "ashikaga/2026-05-29-taishin-kaishu.md";
  slug: "ashikaga/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ashikaga/2026-06-05-shoene-kaishu.md": {
	id: "ashikaga/2026-06-05-shoene-kaishu.md";
  slug: "ashikaga/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ashiya/2026-05-11-shoene.md": {
	id: "ashiya/2026-05-11-shoene.md";
  slug: "ashiya/2026-05-11-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ashiya/2026-05-11-taiyoko.md": {
	id: "ashiya/2026-05-11-taiyoko.md";
  slug: "ashiya/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ashiya/2026-05-29-taishin-kaishu.md": {
	id: "ashiya/2026-05-29-taishin-kaishu.md";
  slug: "ashiya/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"atsugi/2026-05-08-carbon-neutral-hojo.md": {
	id: "atsugi/2026-05-08-carbon-neutral-hojo.md";
  slug: "atsugi/2026-05-08-carbon-neutral-hojo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"atsugi/2026-05-12-chusho.md": {
	id: "atsugi/2026-05-12-chusho.md";
  slug: "atsugi/2026-05-12-chusho";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"atsugi/2026-05-29-taishin-kaishu.md": {
	id: "atsugi/2026-05-29-taishin-kaishu.md";
  slug: "atsugi/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"atsugi/2026-06-05-shoene-kaishu.md": {
	id: "atsugi/2026-06-05-shoene-kaishu.md";
  slug: "atsugi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"awaji/2026-05-11-iju.md": {
	id: "awaji/2026-05-11-iju.md";
  slug: "awaji/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"awaji/2026-05-11-taiyoko.md": {
	id: "awaji/2026-05-11-taiyoko.md";
  slug: "awaji/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"awaji/2026-05-29-taishin-kaishu.md": {
	id: "awaji/2026-05-29-taishin-kaishu.md";
  slug: "awaji/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"awaji/2026-06-05-shoene-kaishu.md": {
	id: "awaji/2026-06-05-shoene-kaishu.md";
  slug: "awaji/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ayabe/2026-05-11-iju.md": {
	id: "ayabe/2026-05-11-iju.md";
  slug: "ayabe/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ayabe/2026-05-11-taiyoko.md": {
	id: "ayabe/2026-05-11-taiyoko.md";
  slug: "ayabe/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ayabe/2026-05-29-taishin-kaishu.md": {
	id: "ayabe/2026-05-29-taishin-kaishu.md";
  slug: "ayabe/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ayabe/2026-06-05-shoene-kaishu.md": {
	id: "ayabe/2026-06-05-shoene-kaishu.md";
  slug: "ayabe/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ayase/2026-05-08-jigyosho-taiyoko.md": {
	id: "ayase/2026-05-08-jigyosho-taiyoko.md";
  slug: "ayase/2026-05-08-jigyosho-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ayase/2026-06-03-taishin-kaishu.md": {
	id: "ayase/2026-06-03-taishin-kaishu.md";
  slug: "ayase/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ayase/2026-06-05-shoene-kaishu.md": {
	id: "ayase/2026-06-05-shoene-kaishu.md";
  slug: "ayase/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"beppu/2026-06-05-shoene-kaishu.md": {
	id: "beppu/2026-06-05-shoene-kaishu.md";
  slug: "beppu/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"beppu/2026-06-05-taishin-kaishu.md": {
	id: "beppu/2026-06-05-taishin-kaishu.md";
  slug: "beppu/2026-06-05-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"bunkyo/2026-05-05-shin-ene-josei.md": {
	id: "bunkyo/2026-05-05-shin-ene-josei.md";
  slug: "bunkyo/2026-05-05-shin-ene-josei";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"bunkyo/2026-05-29-taishin-kaishu.md": {
	id: "bunkyo/2026-05-29-taishin-kaishu.md";
  slug: "bunkyo/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"bunkyo/2026-06-05-shoene-kaishu.md": {
	id: "bunkyo/2026-06-05-shoene-kaishu.md";
  slug: "bunkyo/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chiba/2026-05-23-energy-sienkin.md": {
	id: "chiba/2026-05-23-energy-sienkin.md";
  slug: "chiba/2026-05-23-energy-sienkin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chiba/2026-05-25-kosodate-jutaku.md": {
	id: "chiba/2026-05-25-kosodate-jutaku.md";
  slug: "chiba/2026-05-25-kosodate-jutaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chiba/2026-05-25-taishin-kaishu.md": {
	id: "chiba/2026-05-25-taishin-kaishu.md";
  slug: "chiba/2026-05-25-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chiba/2026-05-25-taiyoko-chikudenchi.md": {
	id: "chiba/2026-05-25-taiyoko-chikudenchi.md";
  slug: "chiba/2026-05-25-taiyoko-chikudenchi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chiba/2026-06-05-shoene-kaishu.md": {
	id: "chiba/2026-06-05-shoene-kaishu.md";
  slug: "chiba/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chiba_pref/2026-05-17-jgrants-cdyjbmah.md": {
	id: "chiba_pref/2026-05-17-jgrants-cdyjbmah.md";
  slug: "chiba_pref/2026-05-17-jgrants-cdyjbmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chiba_pref/2026-05-17-jgrants-cdym2mah.md": {
	id: "chiba_pref/2026-05-17-jgrants-cdym2mah.md";
  slug: "chiba_pref/2026-05-17-jgrants-cdym2mah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chiba_pref/2026-06-03-shoene-kaishu.md": {
	id: "chiba_pref/2026-06-03-shoene-kaishu.md";
  slug: "chiba_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chiba_pref/2026-06-03-taishin-kaishu.md": {
	id: "chiba_pref/2026-06-03-taishin-kaishu.md";
  slug: "chiba_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chichibu/2026-05-10-saiene.md": {
	id: "chichibu/2026-05-10-saiene.md";
  slug: "chichibu/2026-05-10-saiene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chichibu/2026-05-10-sogyo.md": {
	id: "chichibu/2026-05-10-sogyo.md";
  slug: "chichibu/2026-05-10-sogyo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chichibu/2026-05-10-sub2.md": {
	id: "chichibu/2026-05-10-sub2.md";
  slug: "chichibu/2026-05-10-sub2";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chichibu/2026-05-10-taiyoko.md": {
	id: "chichibu/2026-05-10-taiyoko.md";
  slug: "chichibu/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chichibu/2026-06-03-taishin-kaishu.md": {
	id: "chichibu/2026-06-03-taishin-kaishu.md";
  slug: "chichibu/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chichibu/2026-06-05-shoene-kaishu.md": {
	id: "chichibu/2026-06-05-shoene-kaishu.md";
  slug: "chichibu/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chigasaki/2026-05-08-taiyoko-keihatsu.md": {
	id: "chigasaki/2026-05-08-taiyoko-keihatsu.md";
  slug: "chigasaki/2026-05-08-taiyoko-keihatsu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chigasaki/2026-05-12-iju.md": {
	id: "chigasaki/2026-05-12-iju.md";
  slug: "chigasaki/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chigasaki/2026-05-29-taishin-kaishu.md": {
	id: "chigasaki/2026-05-29-taishin-kaishu.md";
  slug: "chigasaki/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chigasaki/2026-06-05-shoene-kaishu.md": {
	id: "chigasaki/2026-06-05-shoene-kaishu.md";
  slug: "chigasaki/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chihayaakasaka/2026-05-04-akiya-kaishu.md": {
	id: "chihayaakasaka/2026-05-04-akiya-kaishu.md";
  slug: "chihayaakasaka/2026-05-04-akiya-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chihayaakasaka/2026-05-04-jokyaku.md": {
	id: "chihayaakasaka/2026-05-04-jokyaku.md";
  slug: "chihayaakasaka/2026-05-04-jokyaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chihayaakasaka/2026-06-03-taishin-kaishu.md": {
	id: "chihayaakasaka/2026-06-03-taishin-kaishu.md";
  slug: "chihayaakasaka/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chihayaakasaka/2026-06-05-shoene-kaishu.md": {
	id: "chihayaakasaka/2026-06-05-shoene-kaishu.md";
  slug: "chihayaakasaka/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chiryu/2026-05-10-jutaku-reform.md": {
	id: "chiryu/2026-05-10-jutaku-reform.md";
  slug: "chiryu/2026-05-10-jutaku-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chiryu/2026-05-10-jutaku-shoene.md": {
	id: "chiryu/2026-05-10-jutaku-shoene.md";
  slug: "chiryu/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chiryu/2026-06-03-taishin-kaishu.md": {
	id: "chiryu/2026-06-03-taishin-kaishu.md";
  slug: "chiryu/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chita/2026-05-10-jutaku-reform.md": {
	id: "chita/2026-05-10-jutaku-reform.md";
  slug: "chita/2026-05-10-jutaku-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chita/2026-05-10-jutaku-shoene.md": {
	id: "chita/2026-05-10-jutaku-shoene.md";
  slug: "chita/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chita/2026-05-29-taishin-kaishu.md": {
	id: "chita/2026-05-29-taishin-kaishu.md";
  slug: "chita/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chiyoda/2026-05-05-sho-ene-kaishu.md": {
	id: "chiyoda/2026-05-05-sho-ene-kaishu.md";
  slug: "chiyoda/2026-05-05-sho-ene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chiyoda/2026-05-29-taishin-kaishu.md": {
	id: "chiyoda/2026-05-29-taishin-kaishu.md";
  slug: "chiyoda/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chiyoda/2026-06-05-shoene-kaishu.md": {
	id: "chiyoda/2026-06-05-shoene-kaishu.md";
  slug: "chiyoda/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chofu/2026-05-06-taishin.md": {
	id: "chofu/2026-05-06-taishin.md";
  slug: "chofu/2026-05-06-taishin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chofu/2026-05-06-yoriyo-sumai.md": {
	id: "chofu/2026-05-06-yoriyo-sumai.md";
  slug: "chofu/2026-05-06-yoriyo-sumai";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chofu/2026-06-05-shoene-kaishu.md": {
	id: "chofu/2026-06-05-shoene-kaishu.md";
  slug: "chofu/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chuo_ku/2026-05-05-shizen-ene-josei.md": {
	id: "chuo_ku/2026-05-05-shizen-ene-josei.md";
  slug: "chuo_ku/2026-05-05-shizen-ene-josei";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chuo_ku/2026-05-29-taishin-kaishu.md": {
	id: "chuo_ku/2026-05-29-taishin-kaishu.md";
  slug: "chuo_ku/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"chuo_ku/2026-06-05-shoene-kaishu.md": {
	id: "chuo_ku/2026-06-05-shoene-kaishu.md";
  slug: "chuo_ku/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"daito/2026-05-01-shoene-jutaku.md": {
	id: "daito/2026-05-01-shoene-jutaku.md";
  slug: "daito/2026-05-01-shoene-jutaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"daito/2026-05-01-shogakukin.md": {
	id: "daito/2026-05-01-shogakukin.md";
  slug: "daito/2026-05-01-shogakukin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"daito/2026-05-29-taishin-kaishu.md": {
	id: "daito/2026-05-29-taishin-kaishu.md";
  slug: "daito/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ebina/2026-05-08-kankyo-hozen.md": {
	id: "ebina/2026-05-08-kankyo-hozen.md";
  slug: "ebina/2026-05-08-kankyo-hozen";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ebina/2026-05-12-chusho.md": {
	id: "ebina/2026-05-12-chusho.md";
  slug: "ebina/2026-05-12-chusho";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ebina/2026-05-29-taishin-kaishu.md": {
	id: "ebina/2026-05-29-taishin-kaishu.md";
  slug: "ebina/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ebina/2026-06-05-shoene-kaishu.md": {
	id: "ebina/2026-06-05-shoene-kaishu.md";
  slug: "ebina/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"edogawa/2026-05-05-saiene-denki.md": {
	id: "edogawa/2026-05-05-saiene-denki.md";
  slug: "edogawa/2026-05-05-saiene-denki";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"edogawa/2026-05-29-taishin-kaishu.md": {
	id: "edogawa/2026-05-29-taishin-kaishu.md";
  slug: "edogawa/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"edogawa/2026-06-05-shoene-kaishu.md": {
	id: "edogawa/2026-06-05-shoene-kaishu.md";
  slug: "edogawa/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ehime_pref/2026-05-27-taishin-kaishu.md": {
	id: "ehime_pref/2026-05-27-taishin-kaishu.md";
  slug: "ehime_pref/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ehime_pref/2026-06-05-shoene-kaishu.md": {
	id: "ehime_pref/2026-06-05-shoene-kaishu.md";
  slug: "ehime_pref/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fuchu/2026-05-06-ecohouse.md": {
	id: "fuchu/2026-05-06-ecohouse.md";
  slug: "fuchu/2026-05-06-ecohouse";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fuchu/2026-05-06-shoene-jigyosya.md": {
	id: "fuchu/2026-05-06-shoene-jigyosya.md";
  slug: "fuchu/2026-05-06-shoene-jigyosya";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fuchu/2026-05-29-taishin-kaishu.md": {
	id: "fuchu/2026-05-29-taishin-kaishu.md";
  slug: "fuchu/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fuji/2026-05-29-akiya-reform.md": {
	id: "fuji/2026-05-29-akiya-reform.md";
  slug: "fuji/2026-05-29-akiya-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fuji/2026-06-03-taishin-kaishu.md": {
	id: "fuji/2026-06-03-taishin-kaishu.md";
  slug: "fuji/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fuji/2026-06-05-shoene-kaishu.md": {
	id: "fuji/2026-06-05-shoene-kaishu.md";
  slug: "fuji/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fujiidera/2026-05-03-jigyosha-shien.md": {
	id: "fujiidera/2026-05-03-jigyosha-shien.md";
  slug: "fujiidera/2026-05-03-jigyosha-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fujiidera/2026-05-03-sogyo-shien.md": {
	id: "fujiidera/2026-05-03-sogyo-shien.md";
  slug: "fujiidera/2026-05-03-sogyo-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fujiidera/2026-06-03-taishin-kaishu.md": {
	id: "fujiidera/2026-06-03-taishin-kaishu.md";
  slug: "fujiidera/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fujiidera/2026-06-05-shoene-kaishu.md": {
	id: "fujiidera/2026-06-05-shoene-kaishu.md";
  slug: "fujiidera/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fujimi/2026-05-10-shoene.md": {
	id: "fujimi/2026-05-10-shoene.md";
  slug: "fujimi/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fujimi/2026-05-10-taiyoko.md": {
	id: "fujimi/2026-05-10-taiyoko.md";
  slug: "fujimi/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fujimi/2026-06-03-taishin-kaishu.md": {
	id: "fujimi/2026-06-03-taishin-kaishu.md";
  slug: "fujimi/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fujimino/2026-05-10-shoene.md": {
	id: "fujimino/2026-05-10-shoene.md";
  slug: "fujimino/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fujimino/2026-05-10-taiyoko.md": {
	id: "fujimino/2026-05-10-taiyoko.md";
  slug: "fujimino/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fujimino/2026-06-03-taishin-kaishu.md": {
	id: "fujimino/2026-06-03-taishin-kaishu.md";
  slug: "fujimino/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fujisawa/2026-05-08-taiyoko-zerocabon.md": {
	id: "fujisawa/2026-05-08-taiyoko-zerocabon.md";
  slug: "fujisawa/2026-05-08-taiyoko-zerocabon";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fujisawa/2026-05-12-kosodatesumai.md": {
	id: "fujisawa/2026-05-12-kosodatesumai.md";
  slug: "fujisawa/2026-05-12-kosodatesumai";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fujisawa/2026-05-29-taishin-kaishu.md": {
	id: "fujisawa/2026-05-29-taishin-kaishu.md";
  slug: "fujisawa/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fujisawa/2026-06-05-shoene-kaishu.md": {
	id: "fujisawa/2026-06-05-shoene-kaishu.md";
  slug: "fujisawa/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukaya/2026-05-10-saiene.md": {
	id: "fukaya/2026-05-10-saiene.md";
  slug: "fukaya/2026-05-10-saiene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukaya/2026-05-10-sogyo.md": {
	id: "fukaya/2026-05-10-sogyo.md";
  slug: "fukaya/2026-05-10-sogyo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukaya/2026-05-10-sub2.md": {
	id: "fukaya/2026-05-10-sub2.md";
  slug: "fukaya/2026-05-10-sub2";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukaya/2026-06-03-taishin-kaishu.md": {
	id: "fukaya/2026-06-03-taishin-kaishu.md";
  slug: "fukaya/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukaya/2026-06-05-shoene-kaishu.md": {
	id: "fukaya/2026-06-05-shoene-kaishu.md";
  slug: "fukaya/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukuchiyama/2026-05-11-iju.md": {
	id: "fukuchiyama/2026-05-11-iju.md";
  slug: "fukuchiyama/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukuchiyama/2026-05-11-taiyoko.md": {
	id: "fukuchiyama/2026-05-11-taiyoko.md";
  slug: "fukuchiyama/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukuchiyama/2026-05-29-taishin-kaishu.md": {
	id: "fukuchiyama/2026-05-29-taishin-kaishu.md";
  slug: "fukuchiyama/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukuchiyama/2026-06-05-shoene-kaishu.md": {
	id: "fukuchiyama/2026-06-05-shoene-kaishu.md";
  slug: "fukuchiyama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukui/2026-05-27-taiyoko.md": {
	id: "fukui/2026-05-27-taiyoko.md";
  slug: "fukui/2026-05-27-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukui/2026-05-29-taishin-kaishu.md": {
	id: "fukui/2026-05-29-taishin-kaishu.md";
  slug: "fukui/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukui/2026-06-05-shoene-kaishu.md": {
	id: "fukui/2026-06-05-shoene-kaishu.md";
  slug: "fukui/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukui/2026-06-05-shoene-sangyou.md": {
	id: "fukui/2026-06-05-shoene-sangyou.md";
  slug: "fukui/2026-06-05-shoene-sangyou";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukui_pref/2026-05-17-jgrants-cdylimah.md": {
	id: "fukui_pref/2026-05-17-jgrants-cdylimah.md";
  slug: "fukui_pref/2026-05-17-jgrants-cdylimah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukui_pref/2026-06-03-shoene-kaishu.md": {
	id: "fukui_pref/2026-06-03-shoene-kaishu.md";
  slug: "fukui_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukui_pref/2026-06-03-taishin-kaishu.md": {
	id: "fukui_pref/2026-06-03-taishin-kaishu.md";
  slug: "fukui_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukuoka/2026-05-23-jutaku-energy.md": {
	id: "fukuoka/2026-05-23-jutaku-energy.md";
  slug: "fukuoka/2026-05-23-jutaku-energy";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukuoka/2026-05-24-energy-system.md": {
	id: "fukuoka/2026-05-24-energy-system.md";
  slug: "fukuoka/2026-05-24-energy-system";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukuoka/2026-05-25-kosodate-jutaku.md": {
	id: "fukuoka/2026-05-25-kosodate-jutaku.md";
  slug: "fukuoka/2026-05-25-kosodate-jutaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukuoka/2026-05-29-taishin-kaishu.md": {
	id: "fukuoka/2026-05-29-taishin-kaishu.md";
  slug: "fukuoka/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukuoka/2026-06-05-shoene-kaishu.md": {
	id: "fukuoka/2026-06-05-shoene-kaishu.md";
  slug: "fukuoka/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukuoka_pref/2026-05-17-jgrants-cdxk8mah.md": {
	id: "fukuoka_pref/2026-05-17-jgrants-cdxk8mah.md";
  slug: "fukuoka_pref/2026-05-17-jgrants-cdxk8mah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukuoka_pref/2026-05-17-jgrants-cdxk9mah.md": {
	id: "fukuoka_pref/2026-05-17-jgrants-cdxk9mah.md";
  slug: "fukuoka_pref/2026-05-17-jgrants-cdxk9mah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukuoka_pref/2026-05-17-jgrants-cdyf9mah.md": {
	id: "fukuoka_pref/2026-05-17-jgrants-cdyf9mah.md";
  slug: "fukuoka_pref/2026-05-17-jgrants-cdyf9mah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukuoka_pref/2026-05-17-jgrants-cdyfwmah.md": {
	id: "fukuoka_pref/2026-05-17-jgrants-cdyfwmah.md";
  slug: "fukuoka_pref/2026-05-17-jgrants-cdyfwmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukuoka_pref/2026-05-17-jgrants-cdyfxmah.md": {
	id: "fukuoka_pref/2026-05-17-jgrants-cdyfxmah.md";
  slug: "fukuoka_pref/2026-05-17-jgrants-cdyfxmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukuoka_pref/2026-05-17-jgrants-cdytvmap.md": {
	id: "fukuoka_pref/2026-05-17-jgrants-cdytvmap.md";
  slug: "fukuoka_pref/2026-05-17-jgrants-cdytvmap";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukuoka_pref/2026-05-17-jgrants-cdyz7map.md": {
	id: "fukuoka_pref/2026-05-17-jgrants-cdyz7map.md";
  slug: "fukuoka_pref/2026-05-17-jgrants-cdyz7map";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukuoka_pref/2026-06-03-shoene-kaishu.md": {
	id: "fukuoka_pref/2026-06-03-shoene-kaishu.md";
  slug: "fukuoka_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukuoka_pref/2026-06-03-taishin-kaishu.md": {
	id: "fukuoka_pref/2026-06-03-taishin-kaishu.md";
  slug: "fukuoka_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukusaki/2026-05-11-iju.md": {
	id: "fukusaki/2026-05-11-iju.md";
  slug: "fukusaki/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukusaki/2026-05-11-taiyoko.md": {
	id: "fukusaki/2026-05-11-taiyoko.md";
  slug: "fukusaki/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukusaki/2026-06-03-taishin-kaishu.md": {
	id: "fukusaki/2026-06-03-taishin-kaishu.md";
  slug: "fukusaki/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukusaki/2026-06-05-shoene-kaishu.md": {
	id: "fukusaki/2026-06-05-shoene-kaishu.md";
  slug: "fukusaki/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukushima/2026-05-27-taishin-kaishu.md": {
	id: "fukushima/2026-05-27-taishin-kaishu.md";
  slug: "fukushima/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukushima/2026-05-27-taiyoko.md": {
	id: "fukushima/2026-05-27-taiyoko.md";
  slug: "fukushima/2026-05-27-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukushima/2026-06-05-shoene-kaishu.md": {
	id: "fukushima/2026-06-05-shoene-kaishu.md";
  slug: "fukushima/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukushima/2026-06-05-shoene-kenshu.md": {
	id: "fukushima/2026-06-05-shoene-kenshu.md";
  slug: "fukushima/2026-06-05-shoene-kenshu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukushima_pref/2026-05-17-jgrants-cdycrma5.md": {
	id: "fukushima_pref/2026-05-17-jgrants-cdycrma5.md";
  slug: "fukushima_pref/2026-05-17-jgrants-cdycrma5";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukushima_pref/2026-05-17-jgrants-cdyhcmax.md": {
	id: "fukushima_pref/2026-05-17-jgrants-cdyhcmax.md";
  slug: "fukushima_pref/2026-05-17-jgrants-cdyhcmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukushima_pref/2026-05-17-jgrants-cdyv1map.md": {
	id: "fukushima_pref/2026-05-17-jgrants-cdyv1map.md";
  slug: "fukushima_pref/2026-05-17-jgrants-cdyv1map";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukushima_pref/2026-05-20-jgrants-cdzbwma5.md": {
	id: "fukushima_pref/2026-05-20-jgrants-cdzbwma5.md";
  slug: "fukushima_pref/2026-05-20-jgrants-cdzbwma5";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukushima_pref/2026-06-03-shoene-kaishu.md": {
	id: "fukushima_pref/2026-06-03-shoene-kaishu.md";
  slug: "fukushima_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukushima_pref/2026-06-03-taishin-kaishu.md": {
	id: "fukushima_pref/2026-06-03-taishin-kaishu.md";
  slug: "fukushima_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukuyama/2026-05-29-taishin-kaishu.md": {
	id: "fukuyama/2026-05-29-taishin-kaishu.md";
  slug: "fukuyama/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fukuyama/2026-06-05-shoene-kaishu.md": {
	id: "fukuyama/2026-06-05-shoene-kaishu.md";
  slug: "fukuyama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"funabashi/2026-05-29-taishin-kaishu.md": {
	id: "funabashi/2026-05-29-taishin-kaishu.md";
  slug: "funabashi/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"funabashi/2026-06-05-shoene-kaishu.md": {
	id: "funabashi/2026-06-05-shoene-kaishu.md";
  slug: "funabashi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fuso/2026-05-10-shoene-aichi-pref.md": {
	id: "fuso/2026-05-10-shoene-aichi-pref.md";
  slug: "fuso/2026-05-10-shoene-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fuso/2026-05-10-sogyo-aichi-pref.md": {
	id: "fuso/2026-05-10-sogyo-aichi-pref.md";
  slug: "fuso/2026-05-10-sogyo-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fuso/2026-06-03-taishin-kaishu.md": {
	id: "fuso/2026-06-03-taishin-kaishu.md";
  slug: "fuso/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fussa/2026-05-12-taiyoko.md": {
	id: "fussa/2026-05-12-taiyoko.md";
  slug: "fussa/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fussa/2026-06-03-taishin-kaishu.md": {
	id: "fussa/2026-06-03-taishin-kaishu.md";
  slug: "fussa/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"fussa/2026-06-05-shoene-kaishu.md": {
	id: "fussa/2026-06-05-shoene-kaishu.md";
  slug: "fussa/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gamagori/2026-05-10-jutaku-reform.md": {
	id: "gamagori/2026-05-10-jutaku-reform.md";
  slug: "gamagori/2026-05-10-jutaku-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gamagori/2026-05-10-jutaku-shoene.md": {
	id: "gamagori/2026-05-10-jutaku-shoene.md";
  slug: "gamagori/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gamagori/2026-05-29-taishin-kaishu.md": {
	id: "gamagori/2026-05-29-taishin-kaishu.md";
  slug: "gamagori/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gifu/2026-05-29-taishin-kaishu.md": {
	id: "gifu/2026-05-29-taishin-kaishu.md";
  slug: "gifu/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gifu/2026-06-05-shoene-kaishu-kenshu.md": {
	id: "gifu/2026-06-05-shoene-kaishu-kenshu.md";
  slug: "gifu/2026-06-05-shoene-kaishu-kenshu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gifu/2026-06-05-shoene-kaishu.md": {
	id: "gifu/2026-06-05-shoene-kaishu.md";
  slug: "gifu/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gifu_pref/2026-05-17-jgrants-cdydtmax.md": {
	id: "gifu_pref/2026-05-17-jgrants-cdydtmax.md";
  slug: "gifu_pref/2026-05-17-jgrants-cdydtmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gifu_pref/2026-06-03-shoene-kaishu.md": {
	id: "gifu_pref/2026-06-03-shoene-kaishu.md";
  slug: "gifu_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gifu_pref/2026-06-03-taishin-kaishu.md": {
	id: "gifu_pref/2026-06-03-taishin-kaishu.md";
  slug: "gifu_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gobo/2026-05-12-taiyoko.md": {
	id: "gobo/2026-05-12-taiyoko.md";
  slug: "gobo/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gobo/2026-06-03-taishin-kaishu.md": {
	id: "gobo/2026-06-03-taishin-kaishu.md";
  slug: "gobo/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gobo/2026-06-05-shoene-kaishu.md": {
	id: "gobo/2026-06-05-shoene-kaishu.md";
  slug: "gobo/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gojo/2026-05-12-iju.md": {
	id: "gojo/2026-05-12-iju.md";
  slug: "gojo/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gojo/2026-05-12-taiyoko.md": {
	id: "gojo/2026-05-12-taiyoko.md";
  slug: "gojo/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gojo/2026-06-03-taishin-kaishu.md": {
	id: "gojo/2026-06-03-taishin-kaishu.md";
  slug: "gojo/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gojo/2026-06-05-shoene-kaishu.md": {
	id: "gojo/2026-06-05-shoene-kaishu.md";
  slug: "gojo/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gose/2026-05-12-taiyoko.md": {
	id: "gose/2026-05-12-taiyoko.md";
  slug: "gose/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gose/2026-06-03-taishin-kaishu.md": {
	id: "gose/2026-06-03-taishin-kaishu.md";
  slug: "gose/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gose/2026-06-05-shoene-kaishu.md": {
	id: "gose/2026-06-05-shoene-kaishu.md";
  slug: "gose/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gunma_pref/2026-05-17-jgrants-cdyzhma5.md": {
	id: "gunma_pref/2026-05-17-jgrants-cdyzhma5.md";
  slug: "gunma_pref/2026-05-17-jgrants-cdyzhma5";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gunma_pref/2026-06-03-shoene-kaishu.md": {
	id: "gunma_pref/2026-06-03-shoene-kaishu.md";
  slug: "gunma_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gunma_pref/2026-06-03-taishin-kaishu.md": {
	id: "gunma_pref/2026-06-03-taishin-kaishu.md";
  slug: "gunma_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gyoda/2026-05-10-saiene.md": {
	id: "gyoda/2026-05-10-saiene.md";
  slug: "gyoda/2026-05-10-saiene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gyoda/2026-05-10-sub2.md": {
	id: "gyoda/2026-05-10-sub2.md";
  slug: "gyoda/2026-05-10-sub2";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gyoda/2026-05-10-taishin.md": {
	id: "gyoda/2026-05-10-taishin.md";
  slug: "gyoda/2026-05-10-taishin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"gyoda/2026-06-05-shoene-kaishu.md": {
	id: "gyoda/2026-06-05-shoene-kaishu.md";
  slug: "gyoda/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"habikino/2026-05-03-sogyo-hojokin.md": {
	id: "habikino/2026-05-03-sogyo-hojokin.md";
  slug: "habikino/2026-05-03-sogyo-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"habikino/2026-05-03-taishin.md": {
	id: "habikino/2026-05-03-taishin.md";
  slug: "habikino/2026-05-03-taishin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"habikino/2026-05-29-taishin-kaishu.md": {
	id: "habikino/2026-05-29-taishin-kaishu.md";
  slug: "habikino/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"habikino/2026-06-05-shoene-kaishu.md": {
	id: "habikino/2026-06-05-shoene-kaishu.md";
  slug: "habikino/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hachinohe/2026-05-29-taishin-kaishu.md": {
	id: "hachinohe/2026-05-29-taishin-kaishu.md";
  slug: "hachinohe/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hachinohe/2026-06-05-shoene-kaishu.md": {
	id: "hachinohe/2026-06-05-shoene-kaishu.md";
  slug: "hachinohe/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hachioji/2026-05-06-kyoju-kankyo.md": {
	id: "hachioji/2026-05-06-kyoju-kankyo.md";
  slug: "hachioji/2026-05-06-kyoju-kankyo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hachioji/2026-05-06-saisei-energy.md": {
	id: "hachioji/2026-05-06-saisei-energy.md";
  slug: "hachioji/2026-05-06-saisei-energy";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hachioji/2026-05-29-taishin-kaishu.md": {
	id: "hachioji/2026-05-29-taishin-kaishu.md";
  slug: "hachioji/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hachioji/2026-06-05-shoene-kaishu.md": {
	id: "hachioji/2026-06-05-shoene-kaishu.md";
  slug: "hachioji/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hadano/2026-05-08-zero-carbon-kurashi.md": {
	id: "hadano/2026-05-08-zero-carbon-kurashi.md";
  slug: "hadano/2026-05-08-zero-carbon-kurashi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hadano/2026-05-12-iju.md": {
	id: "hadano/2026-05-12-iju.md";
  slug: "hadano/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hadano/2026-05-29-taishin-kaishu.md": {
	id: "hadano/2026-05-29-taishin-kaishu.md";
  slug: "hadano/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hadano/2026-06-05-shoene-kaishu.md": {
	id: "hadano/2026-06-05-shoene-kaishu.md";
  slug: "hadano/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hakodate/2026-05-29-taishin-kaishu.md": {
	id: "hakodate/2026-05-29-taishin-kaishu.md";
  slug: "hakodate/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hakodate/2026-06-05-shoene-kaishu.md": {
	id: "hakodate/2026-06-05-shoene-kaishu.md";
  slug: "hakodate/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hakone/2026-05-08-saiene-hojo.md": {
	id: "hakone/2026-05-08-saiene-hojo.md";
  slug: "hakone/2026-05-08-saiene-hojo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hakone/2026-05-12-iju.md": {
	id: "hakone/2026-05-12-iju.md";
  slug: "hakone/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hakone/2026-06-03-taishin-kaishu.md": {
	id: "hakone/2026-06-03-taishin-kaishu.md";
  slug: "hakone/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hakone/2026-06-05-shoene-kaishu.md": {
	id: "hakone/2026-06-05-shoene-kaishu.md";
  slug: "hakone/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hamamatsu/2026-05-24-chusho-yushi.md": {
	id: "hamamatsu/2026-05-24-chusho-yushi.md";
  slug: "hamamatsu/2026-05-24-chusho-yushi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hamamatsu/2026-05-24-smart-house.md": {
	id: "hamamatsu/2026-05-24-smart-house.md";
  slug: "hamamatsu/2026-05-24-smart-house";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hamamatsu/2026-05-27-taishin-kaishu.md": {
	id: "hamamatsu/2026-05-27-taishin-kaishu.md";
  slug: "hamamatsu/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hamamatsu/2026-06-05-shoene-kaishu.md": {
	id: "hamamatsu/2026-06-05-shoene-kaishu.md";
  slug: "hamamatsu/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hamura/2026-05-12-taiyoko.md": {
	id: "hamura/2026-05-12-taiyoko.md";
  slug: "hamura/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hamura/2026-06-03-taishin-kaishu.md": {
	id: "hamura/2026-06-03-taishin-kaishu.md";
  slug: "hamura/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hamura/2026-06-05-shoene-kaishu.md": {
	id: "hamura/2026-06-05-shoene-kaishu.md";
  slug: "hamura/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hanamaki/2026-05-29-taishin-kaishu.md": {
	id: "hanamaki/2026-05-29-taishin-kaishu.md";
  slug: "hanamaki/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hanamaki/2026-06-05-shoene-kaishu.md": {
	id: "hanamaki/2026-06-05-shoene-kaishu.md";
  slug: "hanamaki/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"handa/2026-05-10-jutaku-reform.md": {
	id: "handa/2026-05-10-jutaku-reform.md";
  slug: "handa/2026-05-10-jutaku-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"handa/2026-05-10-jutaku-shoene.md": {
	id: "handa/2026-05-10-jutaku-shoene.md";
  slug: "handa/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"handa/2026-05-29-taishin-kaishu.md": {
	id: "handa/2026-05-29-taishin-kaishu.md";
  slug: "handa/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hannan/2026-05-03-sogyo-voucher.md": {
	id: "hannan/2026-05-03-sogyo-voucher.md";
  slug: "hannan/2026-05-03-sogyo-voucher";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hannan/2026-05-04-akiya-iju.md": {
	id: "hannan/2026-05-04-akiya-iju.md";
  slug: "hannan/2026-05-04-akiya-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hannan/2026-05-04-akiya-jokyaku.md": {
	id: "hannan/2026-05-04-akiya-jokyaku.md";
  slug: "hannan/2026-05-04-akiya-jokyaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hannan/2026-06-03-taishin-kaishu.md": {
	id: "hannan/2026-06-03-taishin-kaishu.md";
  slug: "hannan/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hannan/2026-06-05-shoene-kaishu.md": {
	id: "hannan/2026-06-05-shoene-kaishu.md";
  slug: "hannan/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hanno/2026-05-10-iju.md": {
	id: "hanno/2026-05-10-iju.md";
  slug: "hanno/2026-05-10-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hanno/2026-05-10-saiene.md": {
	id: "hanno/2026-05-10-saiene.md";
  slug: "hanno/2026-05-10-saiene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hanno/2026-06-03-taishin-kaishu.md": {
	id: "hanno/2026-06-03-taishin-kaishu.md";
  slug: "hanno/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hanno/2026-06-05-shoene-kaishu.md": {
	id: "hanno/2026-06-05-shoene-kaishu.md";
  slug: "hanno/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hanyu/2026-05-10-kaden.md": {
	id: "hanyu/2026-05-10-kaden.md";
  slug: "hanyu/2026-05-10-kaden";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hanyu/2026-05-10-saiene.md": {
	id: "hanyu/2026-05-10-saiene.md";
  slug: "hanyu/2026-05-10-saiene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hanyu/2026-05-10-sub2.md": {
	id: "hanyu/2026-05-10-sub2.md";
  slug: "hanyu/2026-05-10-sub2";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hanyu/2026-05-10-taiyoko.md": {
	id: "hanyu/2026-05-10-taiyoko.md";
  slug: "hanyu/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hanyu/2026-06-03-taishin-kaishu.md": {
	id: "hanyu/2026-06-03-taishin-kaishu.md";
  slug: "hanyu/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hanyu/2026-06-05-shoene-kaishu.md": {
	id: "hanyu/2026-06-05-shoene-kaishu.md";
  slug: "hanyu/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"harima/2026-05-11-iju.md": {
	id: "harima/2026-05-11-iju.md";
  slug: "harima/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"harima/2026-05-11-taiyoko.md": {
	id: "harima/2026-05-11-taiyoko.md";
  slug: "harima/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"harima/2026-05-29-taishin-kaishu.md": {
	id: "harima/2026-05-29-taishin-kaishu.md";
  slug: "harima/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"harima/2026-06-05-shoene-kaishu.md": {
	id: "harima/2026-06-05-shoene-kaishu.md";
  slug: "harima/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hashimoto_wakayama/2026-05-12-taiyoko.md": {
	id: "hashimoto_wakayama/2026-05-12-taiyoko.md";
  slug: "hashimoto_wakayama/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hashimoto_wakayama/2026-05-29-taishin-kaishu.md": {
	id: "hashimoto_wakayama/2026-05-29-taishin-kaishu.md";
  slug: "hashimoto_wakayama/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hashimoto_wakayama/2026-06-05-shoene-kaishu.md": {
	id: "hashimoto_wakayama/2026-06-05-shoene-kaishu.md";
  slug: "hashimoto_wakayama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hasuda/2026-05-10-shoene.md": {
	id: "hasuda/2026-05-10-shoene.md";
  slug: "hasuda/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hasuda/2026-05-10-taiyoko.md": {
	id: "hasuda/2026-05-10-taiyoko.md";
  slug: "hasuda/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hasuda/2026-06-03-taishin-kaishu.md": {
	id: "hasuda/2026-06-03-taishin-kaishu.md";
  slug: "hasuda/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hatoyama/2026-05-10-shoene.md": {
	id: "hatoyama/2026-05-10-shoene.md";
  slug: "hatoyama/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hatoyama/2026-05-10-taiyoko.md": {
	id: "hatoyama/2026-05-10-taiyoko.md";
  slug: "hatoyama/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hatoyama/2026-06-03-taishin-kaishu.md": {
	id: "hatoyama/2026-06-03-taishin-kaishu.md";
  slug: "hatoyama/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hayama/2026-05-08-juuten-taiyoko.md": {
	id: "hayama/2026-05-08-juuten-taiyoko.md";
  slug: "hayama/2026-05-08-juuten-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hayama/2026-05-12-iju.md": {
	id: "hayama/2026-05-12-iju.md";
  slug: "hayama/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hayama/2026-06-03-taishin-kaishu.md": {
	id: "hayama/2026-06-03-taishin-kaishu.md";
  slug: "hayama/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hayama/2026-06-05-shoene-kaishu.md": {
	id: "hayama/2026-06-05-shoene-kaishu.md";
  slug: "hayama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hekinan/2026-05-10-jutaku-reform.md": {
	id: "hekinan/2026-05-10-jutaku-reform.md";
  slug: "hekinan/2026-05-10-jutaku-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hekinan/2026-05-10-jutaku-shoene.md": {
	id: "hekinan/2026-05-10-jutaku-shoene.md";
  slug: "hekinan/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hekinan/2026-05-29-taishin-kaishu.md": {
	id: "hekinan/2026-05-29-taishin-kaishu.md";
  slug: "hekinan/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hidaka/2026-05-10-shoene.md": {
	id: "hidaka/2026-05-10-shoene.md";
  slug: "hidaka/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hidaka/2026-05-10-taiyoko.md": {
	id: "hidaka/2026-05-10-taiyoko.md";
  slug: "hidaka/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hidaka/2026-06-03-taishin-kaishu.md": {
	id: "hidaka/2026-06-03-taishin-kaishu.md";
  slug: "hidaka/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashichichibu/2026-05-10-shoene.md": {
	id: "higashichichibu/2026-05-10-shoene.md";
  slug: "higashichichibu/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashichichibu/2026-05-10-taiyoko.md": {
	id: "higashichichibu/2026-05-10-taiyoko.md";
  slug: "higashichichibu/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashichichibu/2026-06-03-taishin-kaishu.md": {
	id: "higashichichibu/2026-06-03-taishin-kaishu.md";
  slug: "higashichichibu/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashihiroshima/2026-05-29-taishin-kaishu.md": {
	id: "higashihiroshima/2026-05-29-taishin-kaishu.md";
  slug: "higashihiroshima/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashihiroshima/2026-06-05-shoene-kaishu.md": {
	id: "higashihiroshima/2026-06-05-shoene-kaishu.md";
  slug: "higashihiroshima/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashikurume/2026-05-12-taiyoko.md": {
	id: "higashikurume/2026-05-12-taiyoko.md";
  slug: "higashikurume/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashikurume/2026-05-29-taishin-kaishu.md": {
	id: "higashikurume/2026-05-29-taishin-kaishu.md";
  slug: "higashikurume/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashikurume/2026-06-05-shoene-kaishu.md": {
	id: "higashikurume/2026-06-05-shoene-kaishu.md";
  slug: "higashikurume/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashimatsuyama/2026-05-10-pref.md": {
	id: "higashimatsuyama/2026-05-10-pref.md";
  slug: "higashimatsuyama/2026-05-10-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashimatsuyama/2026-05-10-taiyoko.md": {
	id: "higashimatsuyama/2026-05-10-taiyoko.md";
  slug: "higashimatsuyama/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashimatsuyama/2026-06-03-taishin-kaishu.md": {
	id: "higashimatsuyama/2026-06-03-taishin-kaishu.md";
  slug: "higashimatsuyama/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashimatsuyama/2026-06-05-shoene-kaishu.md": {
	id: "higashimatsuyama/2026-06-05-shoene-kaishu.md";
  slug: "higashimatsuyama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashimurayama/2026-05-12-taiyoko.md": {
	id: "higashimurayama/2026-05-12-taiyoko.md";
  slug: "higashimurayama/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashimurayama/2026-05-29-taishin-kaishu.md": {
	id: "higashimurayama/2026-05-29-taishin-kaishu.md";
  slug: "higashimurayama/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashimurayama/2026-06-05-shoene-kaishu.md": {
	id: "higashimurayama/2026-06-05-shoene-kaishu.md";
  slug: "higashimurayama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashiomi/2026-05-12-taiyoko.md": {
	id: "higashiomi/2026-05-12-taiyoko.md";
  slug: "higashiomi/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashiomi/2026-05-29-taishin-kaishu.md": {
	id: "higashiomi/2026-05-29-taishin-kaishu.md";
  slug: "higashiomi/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashiomi/2026-06-05-shoene-kaishu.md": {
	id: "higashiomi/2026-06-05-shoene-kaishu.md";
  slug: "higashiomi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashiosaka/2026-04-30-akiya-kaitai.md": {
	id: "higashiosaka/2026-04-30-akiya-kaitai.md";
  slug: "higashiosaka/2026-04-30-akiya-kaitai";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashiosaka/2026-04-30-shoene-setsubi.md": {
	id: "higashiosaka/2026-04-30-shoene-setsubi.md";
  slug: "higashiosaka/2026-04-30-shoene-setsubi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashiosaka/2026-04-30-shogaisha-koyo.md": {
	id: "higashiosaka/2026-04-30-shogaisha-koyo.md";
  slug: "higashiosaka/2026-04-30-shogaisha-koyo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashiosaka/2026-04-30-taishin-kaishu.md": {
	id: "higashiosaka/2026-04-30-taishin-kaishu.md";
  slug: "higashiosaka/2026-04-30-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashiosaka/2026-04-30-trial-koyo.md": {
	id: "higashiosaka/2026-04-30-trial-koyo.md";
  slug: "higashiosaka/2026-04-30-trial-koyo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashiura/2026-05-10-shoene-aichi-pref.md": {
	id: "higashiura/2026-05-10-shoene-aichi-pref.md";
  slug: "higashiura/2026-05-10-shoene-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashiura/2026-05-10-sogyo-aichi-pref.md": {
	id: "higashiura/2026-05-10-sogyo-aichi-pref.md";
  slug: "higashiura/2026-05-10-sogyo-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashiura/2026-06-03-taishin-kaishu.md": {
	id: "higashiura/2026-06-03-taishin-kaishu.md";
  slug: "higashiura/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashiyamato/2026-05-12-taiyoko.md": {
	id: "higashiyamato/2026-05-12-taiyoko.md";
  slug: "higashiyamato/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashiyamato/2026-06-03-taishin-kaishu.md": {
	id: "higashiyamato/2026-06-03-taishin-kaishu.md";
  slug: "higashiyamato/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"higashiyamato/2026-06-05-shoene-kaishu.md": {
	id: "higashiyamato/2026-06-05-shoene-kaishu.md";
  slug: "higashiyamato/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hikone/2026-05-12-taiyoko.md": {
	id: "hikone/2026-05-12-taiyoko.md";
  slug: "hikone/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hikone/2026-05-29-taishin-kaishu.md": {
	id: "hikone/2026-05-29-taishin-kaishu.md";
  slug: "hikone/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hikone/2026-06-05-shoene-kaishu.md": {
	id: "hikone/2026-06-05-shoene-kaishu.md";
  slug: "hikone/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"himeji/2026-05-11-jutaku-reform.md": {
	id: "himeji/2026-05-11-jutaku-reform.md";
  slug: "himeji/2026-05-11-jutaku-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"himeji/2026-05-11-taiyoko.md": {
	id: "himeji/2026-05-11-taiyoko.md";
  slug: "himeji/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"himeji/2026-05-29-taishin-kaishu.md": {
	id: "himeji/2026-05-29-taishin-kaishu.md";
  slug: "himeji/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"himeji/2026-06-05-shoene-kaishu.md": {
	id: "himeji/2026-06-05-shoene-kaishu.md";
  slug: "himeji/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hino/2026-05-12-taiyoko.md": {
	id: "hino/2026-05-12-taiyoko.md";
  slug: "hino/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hino/2026-05-29-taishin-kaishu.md": {
	id: "hino/2026-05-29-taishin-kaishu.md";
  slug: "hino/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hino/2026-06-05-shoene-kaishu.md": {
	id: "hino/2026-06-05-shoene-kaishu.md";
  slug: "hino/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hino_shiga/2026-05-12-taiyoko.md": {
	id: "hino_shiga/2026-05-12-taiyoko.md";
  slug: "hino_shiga/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hino_shiga/2026-05-29-taishin-kaishu.md": {
	id: "hino_shiga/2026-05-29-taishin-kaishu.md";
  slug: "hino_shiga/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hino_shiga/2026-06-05-shoene-kaishu.md": {
	id: "hino_shiga/2026-06-05-shoene-kaishu.md";
  slug: "hino_shiga/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hinode/2026-05-12-taiyoko.md": {
	id: "hinode/2026-05-12-taiyoko.md";
  slug: "hinode/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hinode/2026-06-03-taishin-kaishu.md": {
	id: "hinode/2026-06-03-taishin-kaishu.md";
  slug: "hinode/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hinode/2026-06-05-shoene-kaishu.md": {
	id: "hinode/2026-06-05-shoene-kaishu.md";
  slug: "hinode/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hinohara/2026-05-12-iju.md": {
	id: "hinohara/2026-05-12-iju.md";
  slug: "hinohara/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hinohara/2026-05-12-taiyoko.md": {
	id: "hinohara/2026-05-12-taiyoko.md";
  slug: "hinohara/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hinohara/2026-06-03-taishin-kaishu.md": {
	id: "hinohara/2026-06-03-taishin-kaishu.md";
  slug: "hinohara/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hinohara/2026-06-05-shoene-kaishu.md": {
	id: "hinohara/2026-06-05-shoene-kaishu.md";
  slug: "hinohara/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hirakata/2026-04-30-akiya.md": {
	id: "hirakata/2026-04-30-akiya.md";
  slug: "hirakata/2026-04-30-akiya";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hirakata/2026-04-30-shogakukin.md": {
	id: "hirakata/2026-04-30-shogakukin.md";
  slug: "hirakata/2026-04-30-shogakukin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hirakata/2026-04-30-shotengai.md": {
	id: "hirakata/2026-04-30-shotengai.md";
  slug: "hirakata/2026-04-30-shotengai";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hirakata/2026-04-30-taishin.md": {
	id: "hirakata/2026-04-30-taishin.md";
  slug: "hirakata/2026-04-30-taishin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hirakata/2026-04-30-takeoff.md": {
	id: "hirakata/2026-04-30-takeoff.md";
  slug: "hirakata/2026-04-30-takeoff";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hirakata/2026-06-05-shoene-kaishu.md": {
	id: "hirakata/2026-06-05-shoene-kaishu.md";
  slug: "hirakata/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hiratsuka/2026-05-08-taiyoko-hojo.md": {
	id: "hiratsuka/2026-05-08-taiyoko-hojo.md";
  slug: "hiratsuka/2026-05-08-taiyoko-hojo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hiratsuka/2026-05-29-taishin-kaishu.md": {
	id: "hiratsuka/2026-05-29-taishin-kaishu.md";
  slug: "hiratsuka/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hiratsuka/2026-06-05-shoene-kaishu.md": {
	id: "hiratsuka/2026-06-05-shoene-kaishu.md";
  slug: "hiratsuka/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hiroshima/2026-05-24-akiya-reform-kosodate.md": {
	id: "hiroshima/2026-05-24-akiya-reform-kosodate.md";
  slug: "hiroshima/2026-05-24-akiya-reform-kosodate";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hiroshima/2026-05-24-chusho-yushi.md": {
	id: "hiroshima/2026-05-24-chusho-yushi.md";
  slug: "hiroshima/2026-05-24-chusho-yushi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hiroshima/2026-05-25-ijuu-shienkin.md": {
	id: "hiroshima/2026-05-25-ijuu-shienkin.md";
  slug: "hiroshima/2026-05-25-ijuu-shienkin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hiroshima/2026-05-25-taishin-kaishu.md": {
	id: "hiroshima/2026-05-25-taishin-kaishu.md";
  slug: "hiroshima/2026-05-25-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hiroshima/2026-05-25-taiyoko-chikudenchi.md": {
	id: "hiroshima/2026-05-25-taiyoko-chikudenchi.md";
  slug: "hiroshima/2026-05-25-taiyoko-chikudenchi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hiroshima/2026-06-05-shoene-kaishu.md": {
	id: "hiroshima/2026-06-05-shoene-kaishu.md";
  slug: "hiroshima/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hiroshima_pref/2026-05-17-jgrants-cdycsmap.md": {
	id: "hiroshima_pref/2026-05-17-jgrants-cdycsmap.md";
  slug: "hiroshima_pref/2026-05-17-jgrants-cdycsmap";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hiroshima_pref/2026-05-20-jgrants-cdyqmmax.md": {
	id: "hiroshima_pref/2026-05-20-jgrants-cdyqmmax.md";
  slug: "hiroshima_pref/2026-05-20-jgrants-cdyqmmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hiroshima_pref/2026-06-03-shoene-kaishu.md": {
	id: "hiroshima_pref/2026-06-03-shoene-kaishu.md";
  slug: "hiroshima_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hiroshima_pref/2026-06-03-taishin-kaishu.md": {
	id: "hiroshima_pref/2026-06-03-taishin-kaishu.md";
  slug: "hiroshima_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hitachi/2026-05-29-taishin-kaishu.md": {
	id: "hitachi/2026-05-29-taishin-kaishu.md";
  slug: "hitachi/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hitachi/2026-06-05-shoene-kaishu.md": {
	id: "hitachi/2026-06-05-shoene-kaishu.md";
  slug: "hitachi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hokkaido/2026-05-17-jgrants-cdxrpmap.md": {
	id: "hokkaido/2026-05-17-jgrants-cdxrpmap.md";
  slug: "hokkaido/2026-05-17-jgrants-cdxrpmap";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hokkaido/2026-05-17-jgrants-cdxuemax.md": {
	id: "hokkaido/2026-05-17-jgrants-cdxuemax.md";
  slug: "hokkaido/2026-05-17-jgrants-cdxuemax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hokkaido/2026-05-17-jgrants-cdy2tmax.md": {
	id: "hokkaido/2026-05-17-jgrants-cdy2tmax.md";
  slug: "hokkaido/2026-05-17-jgrants-cdy2tmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hokkaido/2026-05-17-jgrants-cdyh2mah.md": {
	id: "hokkaido/2026-05-17-jgrants-cdyh2mah.md";
  slug: "hokkaido/2026-05-17-jgrants-cdyh2mah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hokkaido/2026-05-17-jgrants-cdyirmah.md": {
	id: "hokkaido/2026-05-17-jgrants-cdyirmah.md";
  slug: "hokkaido/2026-05-17-jgrants-cdyirmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hokkaido/2026-05-17-jgrants-cdymhmah.md": {
	id: "hokkaido/2026-05-17-jgrants-cdymhmah.md";
  slug: "hokkaido/2026-05-17-jgrants-cdymhmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hokkaido/2026-05-17-jgrants-cdymmmah.md": {
	id: "hokkaido/2026-05-17-jgrants-cdymmmah.md";
  slug: "hokkaido/2026-05-17-jgrants-cdymmmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hokkaido/2026-05-17-jgrants-cdymsmax.md": {
	id: "hokkaido/2026-05-17-jgrants-cdymsmax.md";
  slug: "hokkaido/2026-05-17-jgrants-cdymsmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hokkaido/2026-05-17-jgrants-cdymxmax.md": {
	id: "hokkaido/2026-05-17-jgrants-cdymxmax.md";
  slug: "hokkaido/2026-05-17-jgrants-cdymxmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hokkaido/2026-05-17-jgrants-cdyn1mah.md": {
	id: "hokkaido/2026-05-17-jgrants-cdyn1mah.md";
  slug: "hokkaido/2026-05-17-jgrants-cdyn1mah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hokkaido/2026-05-17-jgrants-cdynbmax.md": {
	id: "hokkaido/2026-05-17-jgrants-cdynbmax.md";
  slug: "hokkaido/2026-05-17-jgrants-cdynbmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hokkaido/2026-05-17-jgrants-cdyngmax.md": {
	id: "hokkaido/2026-05-17-jgrants-cdyngmax.md";
  slug: "hokkaido/2026-05-17-jgrants-cdyngmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hokkaido/2026-05-17-jgrants-cdynlmax.md": {
	id: "hokkaido/2026-05-17-jgrants-cdynlmax.md";
  slug: "hokkaido/2026-05-17-jgrants-cdynlmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hokkaido/2026-05-17-jgrants-cdypqma5.md": {
	id: "hokkaido/2026-05-17-jgrants-cdypqma5.md";
  slug: "hokkaido/2026-05-17-jgrants-cdypqma5";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hokkaido/2026-05-17-jgrants-cdyqxmap.md": {
	id: "hokkaido/2026-05-17-jgrants-cdyqxmap.md";
  slug: "hokkaido/2026-05-17-jgrants-cdyqxmap";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hokkaido/2026-05-17-jgrants-cdyr0mah.md": {
	id: "hokkaido/2026-05-17-jgrants-cdyr0mah.md";
  slug: "hokkaido/2026-05-17-jgrants-cdyr0mah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hokkaido/2026-05-17-jgrants-cdyr7map.md": {
	id: "hokkaido/2026-05-17-jgrants-cdyr7map.md";
  slug: "hokkaido/2026-05-17-jgrants-cdyr7map";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hokkaido/2026-05-17-jgrants-cdys1mah.md": {
	id: "hokkaido/2026-05-17-jgrants-cdys1mah.md";
  slug: "hokkaido/2026-05-17-jgrants-cdys1mah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hokkaido/2026-06-03-shoene-kaishu.md": {
	id: "hokkaido/2026-06-03-shoene-kaishu.md";
  slug: "hokkaido/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hokkaido/2026-06-03-taishin-kaishu.md": {
	id: "hokkaido/2026-06-03-taishin-kaishu.md";
  slug: "hokkaido/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"honjo/2026-05-10-reform.md": {
	id: "honjo/2026-05-10-reform.md";
  slug: "honjo/2026-05-10-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"honjo/2026-05-10-taiyoko.md": {
	id: "honjo/2026-05-10-taiyoko.md";
  slug: "honjo/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"honjo/2026-06-03-taishin-kaishu.md": {
	id: "honjo/2026-06-03-taishin-kaishu.md";
  slug: "honjo/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"honjo/2026-06-05-shoene-kaishu.md": {
	id: "honjo/2026-06-05-shoene-kaishu.md";
  slug: "honjo/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hyogo_pref/2026-05-11-chusho-hojokin.md": {
	id: "hyogo_pref/2026-05-11-chusho-hojokin.md";
  slug: "hyogo_pref/2026-05-11-chusho-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hyogo_pref/2026-05-11-jutaku-taiyoko-chikudenchi.md": {
	id: "hyogo_pref/2026-05-11-jutaku-taiyoko-chikudenchi.md";
  slug: "hyogo_pref/2026-05-11-jutaku-taiyoko-chikudenchi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hyogo_pref/2026-05-11-zeh-shoene.md": {
	id: "hyogo_pref/2026-05-11-zeh-shoene.md";
  slug: "hyogo_pref/2026-05-11-zeh-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hyogo_pref/2026-05-20-jgrants-cdyo9mah.md": {
	id: "hyogo_pref/2026-05-20-jgrants-cdyo9mah.md";
  slug: "hyogo_pref/2026-05-20-jgrants-cdyo9mah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hyogo_pref/2026-05-20-jgrants-cdyvymax.md": {
	id: "hyogo_pref/2026-05-20-jgrants-cdyvymax.md";
  slug: "hyogo_pref/2026-05-20-jgrants-cdyvymax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hyogo_pref/2026-06-03-shoene-kaishu.md": {
	id: "hyogo_pref/2026-06-03-shoene-kaishu.md";
  slug: "hyogo_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"hyogo_pref/2026-06-03-taishin-kaishu.md": {
	id: "hyogo_pref/2026-06-03-taishin-kaishu.md";
  slug: "hyogo_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ibaraki/2026-05-01-kinjo-doukyo.md": {
	id: "ibaraki/2026-05-01-kinjo-doukyo.md";
  slug: "ibaraki/2026-05-01-kinjo-doukyo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ibaraki/2026-05-01-shogaisha-koyo.md": {
	id: "ibaraki/2026-05-01-shogaisha-koyo.md";
  slug: "ibaraki/2026-05-01-shogaisha-koyo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ibaraki/2026-05-01-sogyo-rishi.md": {
	id: "ibaraki/2026-05-01-sogyo-rishi.md";
  slug: "ibaraki/2026-05-01-sogyo-rishi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ibaraki/2026-05-29-taishin-kaishu.md": {
	id: "ibaraki/2026-05-29-taishin-kaishu.md";
  slug: "ibaraki/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ibaraki/2026-06-05-shoene-kaishu.md": {
	id: "ibaraki/2026-06-05-shoene-kaishu.md";
  slug: "ibaraki/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ibaraki_pref/2026-05-17-jgrants-cdrafmah.md": {
	id: "ibaraki_pref/2026-05-17-jgrants-cdrafmah.md";
  slug: "ibaraki_pref/2026-05-17-jgrants-cdrafmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ibaraki_pref/2026-05-17-jgrants-cdxobmax.md": {
	id: "ibaraki_pref/2026-05-17-jgrants-cdxobmax.md";
  slug: "ibaraki_pref/2026-05-17-jgrants-cdxobmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ibaraki_pref/2026-05-17-jgrants-cdydamah.md": {
	id: "ibaraki_pref/2026-05-17-jgrants-cdydamah.md";
  slug: "ibaraki_pref/2026-05-17-jgrants-cdydamah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ibaraki_pref/2026-05-17-jgrants-cdydbmah.md": {
	id: "ibaraki_pref/2026-05-17-jgrants-cdydbmah.md";
  slug: "ibaraki_pref/2026-05-17-jgrants-cdydbmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ibaraki_pref/2026-05-17-jgrants-cdydcmah.md": {
	id: "ibaraki_pref/2026-05-17-jgrants-cdydcmah.md";
  slug: "ibaraki_pref/2026-05-17-jgrants-cdydcmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ibaraki_pref/2026-05-17-jgrants-cdyt8map.md": {
	id: "ibaraki_pref/2026-05-17-jgrants-cdyt8map.md";
  slug: "ibaraki_pref/2026-05-17-jgrants-cdyt8map";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ibaraki_pref/2026-06-03-shoene-kaishu.md": {
	id: "ibaraki_pref/2026-06-03-shoene-kaishu.md";
  slug: "ibaraki_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ibaraki_pref/2026-06-03-taishin-kaishu.md": {
	id: "ibaraki_pref/2026-06-03-taishin-kaishu.md";
  slug: "ibaraki_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ichikawa/2026-05-29-taishin-kaishu.md": {
	id: "ichikawa/2026-05-29-taishin-kaishu.md";
  slug: "ichikawa/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ichikawa/2026-06-05-shoene-kaishu.md": {
	id: "ichikawa/2026-06-05-shoene-kaishu.md";
  slug: "ichikawa/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ichikawa_h/2026-05-11-iju.md": {
	id: "ichikawa_h/2026-05-11-iju.md";
  slug: "ichikawa_h/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ichikawa_h/2026-05-11-taiyoko.md": {
	id: "ichikawa_h/2026-05-11-taiyoko.md";
  slug: "ichikawa_h/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ichikawa_h/2026-06-03-taishin-kaishu.md": {
	id: "ichikawa_h/2026-06-03-taishin-kaishu.md";
  slug: "ichikawa_h/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ichikawa_h/2026-06-05-shoene-kaishu.md": {
	id: "ichikawa_h/2026-06-05-shoene-kaishu.md";
  slug: "ichikawa_h/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ichinomiya/2026-05-10-jutaku-shoene.md": {
	id: "ichinomiya/2026-05-10-jutaku-shoene.md";
  slug: "ichinomiya/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ichinomiya/2026-05-10-sogyo-shien.md": {
	id: "ichinomiya/2026-05-10-sogyo-shien.md";
  slug: "ichinomiya/2026-05-10-sogyo-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ichinomiya/2026-05-29-taishin-kaishu.md": {
	id: "ichinomiya/2026-05-29-taishin-kaishu.md";
  slug: "ichinomiya/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ide/2026-05-11-iju.md": {
	id: "ide/2026-05-11-iju.md";
  slug: "ide/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ide/2026-05-11-taiyoko.md": {
	id: "ide/2026-05-11-taiyoko.md";
  slug: "ide/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ide/2026-06-03-taishin-kaishu.md": {
	id: "ide/2026-06-03-taishin-kaishu.md";
  slug: "ide/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ide/2026-06-05-shoene-kaishu.md": {
	id: "ide/2026-06-05-shoene-kaishu.md";
  slug: "ide/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"iida/2026-06-05-shoene-kaishu.md": {
	id: "iida/2026-06-05-shoene-kaishu.md";
  slug: "iida/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"iida/2026-06-05-taishin-kaishu.md": {
	id: "iida/2026-06-05-taishin-kaishu.md";
  slug: "iida/2026-06-05-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ikaruga/2026-05-12-taiyoko.md": {
	id: "ikaruga/2026-05-12-taiyoko.md";
  slug: "ikaruga/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ikaruga/2026-06-03-taishin-kaishu.md": {
	id: "ikaruga/2026-06-03-taishin-kaishu.md";
  slug: "ikaruga/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ikaruga/2026-06-05-shoene-kaishu.md": {
	id: "ikaruga/2026-06-05-shoene-kaishu.md";
  slug: "ikaruga/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ikeda/2026-05-04-akiya-jokyaku.md": {
	id: "ikeda/2026-05-04-akiya-jokyaku.md";
  slug: "ikeda/2026-05-04-akiya-jokyaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ikeda/2026-05-04-solar.md": {
	id: "ikeda/2026-05-04-solar.md";
  slug: "ikeda/2026-05-04-solar";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ikeda/2026-06-03-taishin-kaishu.md": {
	id: "ikeda/2026-06-03-taishin-kaishu.md";
  slug: "ikeda/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ikoma/2026-05-12-taiyoko.md": {
	id: "ikoma/2026-05-12-taiyoko.md";
  slug: "ikoma/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ikoma/2026-05-29-taishin-kaishu.md": {
	id: "ikoma/2026-05-29-taishin-kaishu.md";
  slug: "ikoma/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ikoma/2026-06-05-shoene-kaishu.md": {
	id: "ikoma/2026-06-05-shoene-kaishu.md";
  slug: "ikoma/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"imabari/2026-05-29-taishin-kaishu.md": {
	id: "imabari/2026-05-29-taishin-kaishu.md";
  slug: "imabari/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"imabari/2026-06-05-shoene-kaishu.md": {
	id: "imabari/2026-06-05-shoene-kaishu.md";
  slug: "imabari/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ina_machi/2026-05-10-shoene.md": {
	id: "ina_machi/2026-05-10-shoene.md";
  slug: "ina_machi/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ina_machi/2026-05-10-taiyoko.md": {
	id: "ina_machi/2026-05-10-taiyoko.md";
  slug: "ina_machi/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ina_machi/2026-06-03-taishin-kaishu.md": {
	id: "ina_machi/2026-06-03-taishin-kaishu.md";
  slug: "ina_machi/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"inagawa/2026-05-11-iju.md": {
	id: "inagawa/2026-05-11-iju.md";
  slug: "inagawa/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"inagawa/2026-05-11-taiyoko.md": {
	id: "inagawa/2026-05-11-taiyoko.md";
  slug: "inagawa/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"inagawa/2026-06-03-taishin-kaishu.md": {
	id: "inagawa/2026-06-03-taishin-kaishu.md";
  slug: "inagawa/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"inagawa/2026-06-05-shoene-kaishu.md": {
	id: "inagawa/2026-06-05-shoene-kaishu.md";
  slug: "inagawa/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"inagi/2026-05-12-taiyoko.md": {
	id: "inagi/2026-05-12-taiyoko.md";
  slug: "inagi/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"inagi/2026-06-03-taishin-kaishu.md": {
	id: "inagi/2026-06-03-taishin-kaishu.md";
  slug: "inagi/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"inagi/2026-06-05-shoene-kaishu.md": {
	id: "inagi/2026-06-05-shoene-kaishu.md";
  slug: "inagi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"inami_hyogo/2026-05-11-iju.md": {
	id: "inami_hyogo/2026-05-11-iju.md";
  slug: "inami_hyogo/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"inami_hyogo/2026-05-11-taiyoko.md": {
	id: "inami_hyogo/2026-05-11-taiyoko.md";
  slug: "inami_hyogo/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"inami_hyogo/2026-05-29-taishin-kaishu.md": {
	id: "inami_hyogo/2026-05-29-taishin-kaishu.md";
  slug: "inami_hyogo/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"inami_hyogo/2026-06-05-shoene-kaishu.md": {
	id: "inami_hyogo/2026-06-05-shoene-kaishu.md";
  slug: "inami_hyogo/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"inazawa/2026-05-10-jutaku-shoene.md": {
	id: "inazawa/2026-05-10-jutaku-shoene.md";
  slug: "inazawa/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"inazawa/2026-05-10-sogyo-shien.md": {
	id: "inazawa/2026-05-10-sogyo-shien.md";
  slug: "inazawa/2026-05-10-sogyo-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"inazawa/2026-05-29-taishin-kaishu.md": {
	id: "inazawa/2026-05-29-taishin-kaishu.md";
  slug: "inazawa/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ine/2026-05-11-iju.md": {
	id: "ine/2026-05-11-iju.md";
  slug: "ine/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ine/2026-05-11-taiyoko.md": {
	id: "ine/2026-05-11-taiyoko.md";
  slug: "ine/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ine/2026-06-03-taishin-kaishu.md": {
	id: "ine/2026-06-03-taishin-kaishu.md";
  slug: "ine/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ine/2026-06-05-shoene-kaishu.md": {
	id: "ine/2026-06-05-shoene-kaishu.md";
  slug: "ine/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"inuyama/2026-05-10-jutaku-reform.md": {
	id: "inuyama/2026-05-10-jutaku-reform.md";
  slug: "inuyama/2026-05-10-jutaku-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"inuyama/2026-05-10-jutaku-shoene.md": {
	id: "inuyama/2026-05-10-jutaku-shoene.md";
  slug: "inuyama/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"inuyama/2026-06-03-taishin-kaishu.md": {
	id: "inuyama/2026-06-03-taishin-kaishu.md";
  slug: "inuyama/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"iruma/2026-05-10-shoene.md": {
	id: "iruma/2026-05-10-shoene.md";
  slug: "iruma/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"iruma/2026-05-10-taiyoko.md": {
	id: "iruma/2026-05-10-taiyoko.md";
  slug: "iruma/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"iruma/2026-06-03-taishin-kaishu.md": {
	id: "iruma/2026-06-03-taishin-kaishu.md";
  slug: "iruma/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"isehara/2026-05-08-shoene-jutaku.md": {
	id: "isehara/2026-05-08-shoene-jutaku.md";
  slug: "isehara/2026-05-08-shoene-jutaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"isehara/2026-05-29-taishin-kaishu.md": {
	id: "isehara/2026-05-29-taishin-kaishu.md";
  slug: "isehara/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"isesaki/2026-05-29-taishin-kaishu.md": {
	id: "isesaki/2026-05-29-taishin-kaishu.md";
  slug: "isesaki/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"isesaki/2026-06-05-shoene-kaishu.md": {
	id: "isesaki/2026-06-05-shoene-kaishu.md";
  slug: "isesaki/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ishikawa_pref/2026-05-17-jgrants-17avdeam.md": {
	id: "ishikawa_pref/2026-05-17-jgrants-17avdeam.md";
  slug: "ishikawa_pref/2026-05-17-jgrants-17avdeam";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ishikawa_pref/2026-06-03-shoene-kaishu.md": {
	id: "ishikawa_pref/2026-06-03-shoene-kaishu.md";
  slug: "ishikawa_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ishikawa_pref/2026-06-03-taishin-kaishu.md": {
	id: "ishikawa_pref/2026-06-03-taishin-kaishu.md";
  slug: "ishikawa_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ishinomaki/2026-05-29-taishin-kaishu.md": {
	id: "ishinomaki/2026-05-29-taishin-kaishu.md";
  slug: "ishinomaki/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ishinomaki/2026-06-05-shoene-kaishu.md": {
	id: "ishinomaki/2026-06-05-shoene-kaishu.md";
  slug: "ishinomaki/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"itabashi/2026-05-06-monodukuri-kyosei.md": {
	id: "itabashi/2026-05-06-monodukuri-kyosei.md";
  slug: "itabashi/2026-05-06-monodukuri-kyosei";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"itabashi/2026-05-29-taishin-kaishu.md": {
	id: "itabashi/2026-05-29-taishin-kaishu.md";
  slug: "itabashi/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"itabashi/2026-06-05-shoene-kaishu.md": {
	id: "itabashi/2026-06-05-shoene-kaishu.md";
  slug: "itabashi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"itami/2026-05-11-chusho.md": {
	id: "itami/2026-05-11-chusho.md";
  slug: "itami/2026-05-11-chusho";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"itami/2026-05-11-taiyoko.md": {
	id: "itami/2026-05-11-taiyoko.md";
  slug: "itami/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"itami/2026-05-29-taishin-kaishu.md": {
	id: "itami/2026-05-29-taishin-kaishu.md";
  slug: "itami/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"itami/2026-06-05-shoene-kaishu.md": {
	id: "itami/2026-06-05-shoene-kaishu.md";
  slug: "itami/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"iwade/2026-05-12-taiyoko.md": {
	id: "iwade/2026-05-12-taiyoko.md";
  slug: "iwade/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"iwade/2026-06-03-taishin-kaishu.md": {
	id: "iwade/2026-06-03-taishin-kaishu.md";
  slug: "iwade/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"iwade/2026-06-05-shoene-kaishu.md": {
	id: "iwade/2026-06-05-shoene-kaishu.md";
  slug: "iwade/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"iwaki/2026-05-29-taishin-kaishu.md": {
	id: "iwaki/2026-05-29-taishin-kaishu.md";
  slug: "iwaki/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"iwaki/2026-06-05-shoene-kaishu.md": {
	id: "iwaki/2026-06-05-shoene-kaishu.md";
  slug: "iwaki/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"iwakura/2026-05-10-jutaku-reform.md": {
	id: "iwakura/2026-05-10-jutaku-reform.md";
  slug: "iwakura/2026-05-10-jutaku-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"iwakura/2026-05-10-jutaku-shoene.md": {
	id: "iwakura/2026-05-10-jutaku-shoene.md";
  slug: "iwakura/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"iwakura/2026-06-03-taishin-kaishu.md": {
	id: "iwakura/2026-06-03-taishin-kaishu.md";
  slug: "iwakura/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"iwate_pref/2026-05-22-jgrants-cdzdmma5.md": {
	id: "iwate_pref/2026-05-22-jgrants-cdzdmma5.md";
  slug: "iwate_pref/2026-05-22-jgrants-cdzdmma5";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"iwate_pref/2026-06-03-shoene-kaishu.md": {
	id: "iwate_pref/2026-06-03-shoene-kaishu.md";
  slug: "iwate_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"iwate_pref/2026-06-03-taishin-kaishu.md": {
	id: "iwate_pref/2026-06-03-taishin-kaishu.md";
  slug: "iwate_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"izumiotsu/2026-05-03-inbound.md": {
	id: "izumiotsu/2026-05-03-inbound.md";
  slug: "izumiotsu/2026-05-03-inbound";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"izumiotsu/2026-05-03-zero-carbon.md": {
	id: "izumiotsu/2026-05-03-zero-carbon.md";
  slug: "izumiotsu/2026-05-03-zero-carbon";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"izumiotsu/2026-05-29-taishin-kaishu.md": {
	id: "izumiotsu/2026-05-29-taishin-kaishu.md";
  slug: "izumiotsu/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"izumiotsu/2026-06-05-shoene-kaishu.md": {
	id: "izumiotsu/2026-06-05-shoene-kaishu.md";
  slug: "izumiotsu/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"izumisano/2026-05-03-chusho-sogo.md": {
	id: "izumisano/2026-05-03-chusho-sogo.md";
  slug: "izumisano/2026-05-03-chusho-sogo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"izumisano/2026-05-04-jutaku-sogo.md": {
	id: "izumisano/2026-05-04-jutaku-sogo.md";
  slug: "izumisano/2026-05-04-jutaku-sogo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"izumisano/2026-06-03-taishin-kaishu.md": {
	id: "izumisano/2026-06-03-taishin-kaishu.md";
  slug: "izumisano/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"izumisano/2026-06-05-shoene-kaishu.md": {
	id: "izumisano/2026-06-05-shoene-kaishu.md";
  slug: "izumisano/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"joetsu/2026-05-29-reform-kosodate.md": {
	id: "joetsu/2026-05-29-reform-kosodate.md";
  slug: "joetsu/2026-05-29-reform-kosodate";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"joetsu/2026-05-29-taishin-kaishu.md": {
	id: "joetsu/2026-05-29-taishin-kaishu.md";
  slug: "joetsu/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"joetsu/2026-06-05-shoene-kaishu.md": {
	id: "joetsu/2026-06-05-shoene-kaishu.md";
  slug: "joetsu/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"joyo/2026-05-11-taiyoko.md": {
	id: "joyo/2026-05-11-taiyoko.md";
  slug: "joyo/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"joyo/2026-05-29-taishin-kaishu.md": {
	id: "joyo/2026-05-29-taishin-kaishu.md";
  slug: "joyo/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"joyo/2026-06-05-shoene-kaishu.md": {
	id: "joyo/2026-06-05-shoene-kaishu.md";
  slug: "joyo/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kadoma/2026-05-01-iten-hojokin.md": {
	id: "kadoma/2026-05-01-iten-hojokin.md";
  slug: "kadoma/2026-05-01-iten-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kadoma/2026-05-01-miryoku-up.md": {
	id: "kadoma/2026-05-01-miryoku-up.md";
  slug: "kadoma/2026-05-01-miryoku-up";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kadoma/2026-05-01-shogyo-shinko.md": {
	id: "kadoma/2026-05-01-shogyo-shinko.md";
  slug: "kadoma/2026-05-01-shogyo-shinko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kadoma/2026-05-29-taishin-kaishu.md": {
	id: "kadoma/2026-05-29-taishin-kaishu.md";
  slug: "kadoma/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kadoma/2026-06-05-shoene-kaishu.md": {
	id: "kadoma/2026-06-05-shoene-kaishu.md";
  slug: "kadoma/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kagawa_pref/2026-05-20-jgrants-cdytomah.md": {
	id: "kagawa_pref/2026-05-20-jgrants-cdytomah.md";
  slug: "kagawa_pref/2026-05-20-jgrants-cdytomah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kagawa_pref/2026-06-03-shoene-kaishu.md": {
	id: "kagawa_pref/2026-06-03-shoene-kaishu.md";
  slug: "kagawa_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kagawa_pref/2026-06-03-taishin-kaishu.md": {
	id: "kagawa_pref/2026-06-03-taishin-kaishu.md";
  slug: "kagawa_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kagoshima/2026-05-23-chusho-yushi-ict.md": {
	id: "kagoshima/2026-05-23-chusho-yushi-ict.md";
  slug: "kagoshima/2026-05-23-chusho-yushi-ict";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kagoshima/2026-05-25-ijuu-shienkin.md": {
	id: "kagoshima/2026-05-25-ijuu-shienkin.md";
  slug: "kagoshima/2026-05-25-ijuu-shienkin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kagoshima/2026-05-29-taishin-kaishu.md": {
	id: "kagoshima/2026-05-29-taishin-kaishu.md";
  slug: "kagoshima/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kagoshima/2026-06-05-shoene-kaishu.md": {
	id: "kagoshima/2026-06-05-shoene-kaishu.md";
  slug: "kagoshima/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kagoshima/2026-06-05-shoene-zeikin.md": {
	id: "kagoshima/2026-06-05-shoene-zeikin.md";
  slug: "kagoshima/2026-06-05-shoene-zeikin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kagoshima_pref/2026-05-27-iju-shienkin.md": {
	id: "kagoshima_pref/2026-05-27-iju-shienkin.md";
  slug: "kagoshima_pref/2026-05-27-iju-shienkin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kagoshima_pref/2026-06-03-shoene-kaishu.md": {
	id: "kagoshima_pref/2026-06-03-shoene-kaishu.md";
  slug: "kagoshima_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kagoshima_pref/2026-06-03-taishin-kaishu.md": {
	id: "kagoshima_pref/2026-06-03-taishin-kaishu.md";
  slug: "kagoshima_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kainan/2026-05-12-taiyoko.md": {
	id: "kainan/2026-05-12-taiyoko.md";
  slug: "kainan/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kainan/2026-06-03-taishin-kaishu.md": {
	id: "kainan/2026-06-03-taishin-kaishu.md";
  slug: "kainan/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kainan/2026-06-05-shoene-kaishu.md": {
	id: "kainan/2026-06-05-shoene-kaishu.md";
  slug: "kainan/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kaisei/2026-05-08-chusho-gx.md": {
	id: "kaisei/2026-05-08-chusho-gx.md";
  slug: "kaisei/2026-05-08-chusho-gx";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kaisei/2026-06-03-taishin-kaishu.md": {
	id: "kaisei/2026-06-03-taishin-kaishu.md";
  slug: "kaisei/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kaisei/2026-06-05-shoene-kaishu.md": {
	id: "kaisei/2026-06-05-shoene-kaishu.md";
  slug: "kaisei/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kaizuka/2026-05-04-shoene.md": {
	id: "kaizuka/2026-05-04-shoene.md";
  slug: "kaizuka/2026-05-04-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kaizuka/2026-05-04-wakane-teiju.md": {
	id: "kaizuka/2026-05-04-wakane-teiju.md";
  slug: "kaizuka/2026-05-04-wakane-teiju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kaizuka/2026-06-03-taishin-kaishu.md": {
	id: "kaizuka/2026-06-03-taishin-kaishu.md";
  slug: "kaizuka/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kakogawa/2026-05-11-chusho.md": {
	id: "kakogawa/2026-05-11-chusho.md";
  slug: "kakogawa/2026-05-11-chusho";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kakogawa/2026-05-11-taiyoko.md": {
	id: "kakogawa/2026-05-11-taiyoko.md";
  slug: "kakogawa/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kakogawa/2026-05-29-taishin-kaishu.md": {
	id: "kakogawa/2026-05-29-taishin-kaishu.md";
  slug: "kakogawa/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kakogawa/2026-06-05-shoene-kaishu.md": {
	id: "kakogawa/2026-06-05-shoene-kaishu.md";
  slug: "kakogawa/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kamakura/2026-05-08-juuten-taiyoko.md": {
	id: "kamakura/2026-05-08-juuten-taiyoko.md";
  slug: "kamakura/2026-05-08-juuten-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kamakura/2026-05-12-iju.md": {
	id: "kamakura/2026-05-12-iju.md";
  slug: "kamakura/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kamakura/2026-05-29-taishin-kaishu.md": {
	id: "kamakura/2026-05-29-taishin-kaishu.md";
  slug: "kamakura/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kamakura/2026-06-05-shoene-kaishu.md": {
	id: "kamakura/2026-06-05-shoene-kaishu.md";
  slug: "kamakura/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kameoka/2026-05-11-iju.md": {
	id: "kameoka/2026-05-11-iju.md";
  slug: "kameoka/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kameoka/2026-05-11-taiyoko.md": {
	id: "kameoka/2026-05-11-taiyoko.md";
  slug: "kameoka/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kameoka/2026-05-29-taishin-kaishu.md": {
	id: "kameoka/2026-05-29-taishin-kaishu.md";
  slug: "kameoka/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kameoka/2026-06-05-shoene-kaishu.md": {
	id: "kameoka/2026-06-05-shoene-kaishu.md";
  slug: "kameoka/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kamigori/2026-05-11-iju.md": {
	id: "kamigori/2026-05-11-iju.md";
  slug: "kamigori/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kamigori/2026-05-11-taiyoko.md": {
	id: "kamigori/2026-05-11-taiyoko.md";
  slug: "kamigori/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kamigori/2026-06-03-taishin-kaishu.md": {
	id: "kamigori/2026-06-03-taishin-kaishu.md";
  slug: "kamigori/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kamigori/2026-06-05-shoene-kaishu.md": {
	id: "kamigori/2026-06-05-shoene-kaishu.md";
  slug: "kamigori/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kamikawa_h/2026-05-11-iju.md": {
	id: "kamikawa_h/2026-05-11-iju.md";
  slug: "kamikawa_h/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kamikawa_h/2026-05-11-taiyoko.md": {
	id: "kamikawa_h/2026-05-11-taiyoko.md";
  slug: "kamikawa_h/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kamikawa_h/2026-06-03-taishin-kaishu.md": {
	id: "kamikawa_h/2026-06-03-taishin-kaishu.md";
  slug: "kamikawa_h/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kamikawa_h/2026-06-05-shoene-kaishu.md": {
	id: "kamikawa_h/2026-06-05-shoene-kaishu.md";
  slug: "kamikawa_h/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kamikawa_s/2026-05-10-shoene.md": {
	id: "kamikawa_s/2026-05-10-shoene.md";
  slug: "kamikawa_s/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kamikawa_s/2026-05-10-taiyoko.md": {
	id: "kamikawa_s/2026-05-10-taiyoko.md";
  slug: "kamikawa_s/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kamikawa_s/2026-05-29-taishin-kaishu.md": {
	id: "kamikawa_s/2026-05-29-taishin-kaishu.md";
  slug: "kamikawa_s/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kamisato/2026-05-10-shoene.md": {
	id: "kamisato/2026-05-10-shoene.md";
  slug: "kamisato/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kamisato/2026-05-10-taiyoko.md": {
	id: "kamisato/2026-05-10-taiyoko.md";
  slug: "kamisato/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kamisato/2026-06-03-taishin-kaishu.md": {
	id: "kamisato/2026-06-03-taishin-kaishu.md";
  slug: "kamisato/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kanagawa_pref/2026-05-08-jikashouhi-saiene.md": {
	id: "kanagawa_pref/2026-05-08-jikashouhi-saiene.md";
  slug: "kanagawa_pref/2026-05-08-jikashouhi-saiene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kanagawa_pref/2026-05-08-jutaku-taiyoko-chikudenchi.md": {
	id: "kanagawa_pref/2026-05-08-jutaku-taiyoko-chikudenchi.md";
  slug: "kanagawa_pref/2026-05-08-jutaku-taiyoko-chikudenchi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kanagawa_pref/2026-05-08-shokibo-digital.md": {
	id: "kanagawa_pref/2026-05-08-shokibo-digital.md";
  slug: "kanagawa_pref/2026-05-08-shokibo-digital";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kanagawa_pref/2026-05-17-jgrants-cdyehma5.md": {
	id: "kanagawa_pref/2026-05-17-jgrants-cdyehma5.md";
  slug: "kanagawa_pref/2026-05-17-jgrants-cdyehma5";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kanagawa_pref/2026-05-17-jgrants-cdyeima5.md": {
	id: "kanagawa_pref/2026-05-17-jgrants-cdyeima5.md";
  slug: "kanagawa_pref/2026-05-17-jgrants-cdyeima5";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kanagawa_pref/2026-06-03-shoene-kaishu.md": {
	id: "kanagawa_pref/2026-06-03-shoene-kaishu.md";
  slug: "kanagawa_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kanagawa_pref/2026-06-03-taishin-kaishu.md": {
	id: "kanagawa_pref/2026-06-03-taishin-kaishu.md";
  slug: "kanagawa_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kanan/2026-05-04-venture.md": {
	id: "kanan/2026-05-04-venture.md";
  slug: "kanan/2026-05-04-venture";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kanan/2026-06-03-taishin-kaishu.md": {
	id: "kanan/2026-06-03-taishin-kaishu.md";
  slug: "kanan/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kanan/2026-06-05-shoene-kaishu.md": {
	id: "kanan/2026-06-05-shoene-kaishu.md";
  slug: "kanan/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kanazawa/2026-05-27-iju-shienkin.md": {
	id: "kanazawa/2026-05-27-iju-shienkin.md";
  slug: "kanazawa/2026-05-27-iju-shienkin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kanazawa/2026-05-27-taishin-kaishu.md": {
	id: "kanazawa/2026-05-27-taishin-kaishu.md";
  slug: "kanazawa/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kanazawa/2026-06-05-shoene-kaishu.md": {
	id: "kanazawa/2026-06-05-shoene-kaishu.md";
  slug: "kanazawa/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kanazawa/2026-06-05-soene-hojo.md": {
	id: "kanazawa/2026-06-05-soene-hojo.md";
  slug: "kanazawa/2026-06-05-soene-hojo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kanie/2026-05-10-shoene-aichi-pref.md": {
	id: "kanie/2026-05-10-shoene-aichi-pref.md";
  slug: "kanie/2026-05-10-shoene-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kanie/2026-05-10-sogyo-aichi-pref.md": {
	id: "kanie/2026-05-10-sogyo-aichi-pref.md";
  slug: "kanie/2026-05-10-sogyo-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kanie/2026-06-03-taishin-kaishu.md": {
	id: "kanie/2026-06-03-taishin-kaishu.md";
  slug: "kanie/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kariya/2026-05-10-jutaku-shoene.md": {
	id: "kariya/2026-05-10-jutaku-shoene.md";
  slug: "kariya/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kariya/2026-05-10-sogyo-shien.md": {
	id: "kariya/2026-05-10-sogyo-shien.md";
  slug: "kariya/2026-05-10-sogyo-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kariya/2026-05-29-taishin-kaishu.md": {
	id: "kariya/2026-05-29-taishin-kaishu.md";
  slug: "kariya/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kasagi/2026-05-11-iju.md": {
	id: "kasagi/2026-05-11-iju.md";
  slug: "kasagi/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kasagi/2026-05-11-taiyoko.md": {
	id: "kasagi/2026-05-11-taiyoko.md";
  slug: "kasagi/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kasagi/2026-06-03-taishin-kaishu.md": {
	id: "kasagi/2026-06-03-taishin-kaishu.md";
  slug: "kasagi/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kasagi/2026-06-05-shoene-kaishu.md": {
	id: "kasagi/2026-06-05-shoene-kaishu.md";
  slug: "kasagi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kasai/2026-05-11-akiya.md": {
	id: "kasai/2026-05-11-akiya.md";
  slug: "kasai/2026-05-11-akiya";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kasai/2026-05-11-taiyoko.md": {
	id: "kasai/2026-05-11-taiyoko.md";
  slug: "kasai/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kasai/2026-06-03-taishin-kaishu.md": {
	id: "kasai/2026-06-03-taishin-kaishu.md";
  slug: "kasai/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kasai/2026-06-05-shoene-kaishu.md": {
	id: "kasai/2026-06-05-shoene-kaishu.md";
  slug: "kasai/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kashiba/2026-05-12-taiyoko.md": {
	id: "kashiba/2026-05-12-taiyoko.md";
  slug: "kashiba/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kashiba/2026-06-03-taishin-kaishu.md": {
	id: "kashiba/2026-06-03-taishin-kaishu.md";
  slug: "kashiba/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kashiba/2026-06-05-shoene-kaishu.md": {
	id: "kashiba/2026-06-05-shoene-kaishu.md";
  slug: "kashiba/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kashihara/2026-05-12-taiyoko.md": {
	id: "kashihara/2026-05-12-taiyoko.md";
  slug: "kashihara/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kashihara/2026-05-29-taishin-kaishu.md": {
	id: "kashihara/2026-05-29-taishin-kaishu.md";
  slug: "kashihara/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kashihara/2026-06-05-shoene-kaishu.md": {
	id: "kashihara/2026-06-05-shoene-kaishu.md";
  slug: "kashihara/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kashiwa/2026-05-29-taishin-kaishu.md": {
	id: "kashiwa/2026-05-29-taishin-kaishu.md";
  slug: "kashiwa/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kashiwa/2026-06-05-shoene-kaishu.md": {
	id: "kashiwa/2026-06-05-shoene-kaishu.md";
  slug: "kashiwa/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kashiwara/2026-05-01-kigyoritchi.md": {
	id: "kashiwara/2026-05-01-kigyoritchi.md";
  slug: "kashiwara/2026-05-01-kigyoritchi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kashiwara/2026-05-01-shinki-shuten.md": {
	id: "kashiwara/2026-05-01-shinki-shuten.md";
  slug: "kashiwara/2026-05-01-shinki-shuten";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kashiwara/2026-05-01-taishin.md": {
	id: "kashiwara/2026-05-01-taishin.md";
  slug: "kashiwara/2026-05-01-taishin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kashiwara/2026-06-05-shoene-kaishu.md": {
	id: "kashiwara/2026-06-05-shoene-kaishu.md";
  slug: "kashiwara/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kasugai/2026-05-10-jutaku-shoene.md": {
	id: "kasugai/2026-05-10-jutaku-shoene.md";
  slug: "kasugai/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kasugai/2026-05-10-jutaku-shutoku.md": {
	id: "kasugai/2026-05-10-jutaku-shutoku.md";
  slug: "kasugai/2026-05-10-jutaku-shutoku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kasugai/2026-05-10-sogyo-shien.md": {
	id: "kasugai/2026-05-10-sogyo-shien.md";
  slug: "kasugai/2026-05-10-sogyo-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kasugai/2026-05-29-taishin-kaishu.md": {
	id: "kasugai/2026-05-29-taishin-kaishu.md";
  slug: "kasugai/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kasukabe/2026-05-10-jigyosha.md": {
	id: "kasukabe/2026-05-10-jigyosha.md";
  slug: "kasukabe/2026-05-10-jigyosha";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kasukabe/2026-05-10-taiyoko.md": {
	id: "kasukabe/2026-05-10-taiyoko.md";
  slug: "kasukabe/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kasukabe/2026-05-29-taishin-kaishu.md": {
	id: "kasukabe/2026-05-29-taishin-kaishu.md";
  slug: "kasukabe/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kasukabe/2026-06-05-shoene-kaishu.md": {
	id: "kasukabe/2026-06-05-shoene-kaishu.md";
  slug: "kasukabe/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"katano/2026-05-01-sangyo-shinko.md": {
	id: "katano/2026-05-01-sangyo-shinko.md";
  slug: "katano/2026-05-01-sangyo-shinko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"katano/2026-05-04-chuko-jutaku.md": {
	id: "katano/2026-05-04-chuko-jutaku.md";
  slug: "katano/2026-05-04-chuko-jutaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"katano/2026-06-03-taishin-kaishu.md": {
	id: "katano/2026-06-03-taishin-kaishu.md";
  slug: "katano/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"katano/2026-06-05-shoene-kaishu.md": {
	id: "katano/2026-06-05-shoene-kaishu.md";
  slug: "katano/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kato/2026-05-11-taiyoko.md": {
	id: "kato/2026-05-11-taiyoko.md";
  slug: "kato/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kato/2026-05-11-teijyu.md": {
	id: "kato/2026-05-11-teijyu.md";
  slug: "kato/2026-05-11-teijyu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kato/2026-05-29-taishin-kaishu.md": {
	id: "kato/2026-05-29-taishin-kaishu.md";
  slug: "kato/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kato/2026-06-05-shoene-kaishu.md": {
	id: "kato/2026-06-05-shoene-kaishu.md";
  slug: "kato/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"katsuragi/2026-05-12-taiyoko.md": {
	id: "katsuragi/2026-05-12-taiyoko.md";
  slug: "katsuragi/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"katsuragi/2026-05-29-taishin-kaishu.md": {
	id: "katsuragi/2026-05-29-taishin-kaishu.md";
  slug: "katsuragi/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"katsuragi/2026-06-05-shoene-kaishu.md": {
	id: "katsuragi/2026-06-05-shoene-kaishu.md";
  slug: "katsuragi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"katsushika/2026-05-05-eco-josei.md": {
	id: "katsushika/2026-05-05-eco-josei.md";
  slug: "katsushika/2026-05-05-eco-josei";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"katsushika/2026-05-29-taishin-kaishu.md": {
	id: "katsushika/2026-05-29-taishin-kaishu.md";
  slug: "katsushika/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"katsushika/2026-06-05-shoene-kaishu.md": {
	id: "katsushika/2026-06-05-shoene-kaishu.md";
  slug: "katsushika/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawachinagano/2026-05-03-kawachizai.md": {
	id: "kawachinagano/2026-05-03-kawachizai.md";
  slug: "kawachinagano/2026-05-03-kawachizai";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawachinagano/2026-05-03-myhome.md": {
	id: "kawachinagano/2026-05-03-myhome.md";
  slug: "kawachinagano/2026-05-03-myhome";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawachinagano/2026-06-03-taishin-kaishu.md": {
	id: "kawachinagano/2026-06-03-taishin-kaishu.md";
  slug: "kawachinagano/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawachinagano/2026-06-05-shoene-kaishu.md": {
	id: "kawachinagano/2026-06-05-shoene-kaishu.md";
  slug: "kawachinagano/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawagoe/2026-05-10-datsutanso.md": {
	id: "kawagoe/2026-05-10-datsutanso.md";
  slug: "kawagoe/2026-05-10-datsutanso";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawagoe/2026-05-10-jutaku-reform.md": {
	id: "kawagoe/2026-05-10-jutaku-reform.md";
  slug: "kawagoe/2026-05-10-jutaku-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawagoe/2026-05-10-reform.md": {
	id: "kawagoe/2026-05-10-reform.md";
  slug: "kawagoe/2026-05-10-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawagoe/2026-05-29-taishin-kaishu.md": {
	id: "kawagoe/2026-05-29-taishin-kaishu.md";
  slug: "kawagoe/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawagoe/2026-06-05-shoene-kaishu.md": {
	id: "kawagoe/2026-06-05-shoene-kaishu.md";
  slug: "kawagoe/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawaguchi/2026-05-10-chikyu-ondanka.md": {
	id: "kawaguchi/2026-05-10-chikyu-ondanka.md";
  slug: "kawaguchi/2026-05-10-chikyu-ondanka";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawaguchi/2026-05-10-ondanka.md": {
	id: "kawaguchi/2026-05-10-ondanka.md";
  slug: "kawaguchi/2026-05-10-ondanka";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawaguchi/2026-05-10-reform.md": {
	id: "kawaguchi/2026-05-10-reform.md";
  slug: "kawaguchi/2026-05-10-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawaguchi/2026-05-29-taishin-kaishu.md": {
	id: "kawaguchi/2026-05-29-taishin-kaishu.md";
  slug: "kawaguchi/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawaguchi/2026-06-05-shoene-kaishu.md": {
	id: "kawaguchi/2026-06-05-shoene-kaishu.md";
  slug: "kawaguchi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawanishi/2026-05-11-taiyoko.md": {
	id: "kawanishi/2026-05-11-taiyoko.md";
  slug: "kawanishi/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawanishi/2026-05-11-zeh.md": {
	id: "kawanishi/2026-05-11-zeh.md";
  slug: "kawanishi/2026-05-11-zeh";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawanishi/2026-05-29-taishin-kaishu.md": {
	id: "kawanishi/2026-05-29-taishin-kaishu.md";
  slug: "kawanishi/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawasaki/2026-05-08-taiyoko-setchi.md": {
	id: "kawasaki/2026-05-08-taiyoko-setchi.md";
  slug: "kawasaki/2026-05-08-taiyoko-setchi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawasaki/2026-05-12-kosodatesumai.md": {
	id: "kawasaki/2026-05-12-kosodatesumai.md";
  slug: "kawasaki/2026-05-12-kosodatesumai";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawasaki/2026-05-20-chuusho-seichou-hojokin.md": {
	id: "kawasaki/2026-05-20-chuusho-seichou-hojokin.md";
  slug: "kawasaki/2026-05-20-chuusho-seichou-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawasaki/2026-05-20-eco-shien-hojokin.md": {
	id: "kawasaki/2026-05-20-eco-shien-hojokin.md";
  slug: "kawasaki/2026-05-20-eco-shien-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawasaki/2026-05-20-esg-finance.md": {
	id: "kawasaki/2026-05-20-esg-finance.md";
  slug: "kawasaki/2026-05-20-esg-finance";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawasaki/2026-05-20-nougyou-seisankoujou.md": {
	id: "kawasaki/2026-05-20-nougyou-seisankoujou.md";
  slug: "kawasaki/2026-05-20-nougyou-seisankoujou";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawasaki/2026-05-20-shoutengai-kadai.md": {
	id: "kawasaki/2026-05-20-shoutengai-kadai.md";
  slug: "kawasaki/2026-05-20-shoutengai-kadai";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawasaki/2026-05-27-taishin-kaishu.md": {
	id: "kawasaki/2026-05-27-taishin-kaishu.md";
  slug: "kawasaki/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawasaki/2026-06-05-shoene-kaishu.md": {
	id: "kawasaki/2026-06-05-shoene-kaishu.md";
  slug: "kawasaki/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawasaki/2026-06-05-shoene-taisyoku-hojo.md": {
	id: "kawasaki/2026-06-05-shoene-taisyoku-hojo.md";
  slug: "kawasaki/2026-06-05-shoene-taisyoku-hojo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawashima_s/2026-05-10-shoene.md": {
	id: "kawashima_s/2026-05-10-shoene.md";
  slug: "kawashima_s/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawashima_s/2026-05-10-taiyoko.md": {
	id: "kawashima_s/2026-05-10-taiyoko.md";
  slug: "kawashima_s/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kawashima_s/2026-06-03-taishin-kaishu.md": {
	id: "kawashima_s/2026-06-03-taishin-kaishu.md";
  slug: "kawashima_s/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kazo/2026-05-10-saiene.md": {
	id: "kazo/2026-05-10-saiene.md";
  slug: "kazo/2026-05-10-saiene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kazo/2026-05-10-sogyo.md": {
	id: "kazo/2026-05-10-sogyo.md";
  slug: "kazo/2026-05-10-sogyo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kazo/2026-06-03-taishin-kaishu.md": {
	id: "kazo/2026-06-03-taishin-kaishu.md";
  slug: "kazo/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kazo/2026-06-05-shoene-kaishu.md": {
	id: "kazo/2026-06-05-shoene-kaishu.md";
  slug: "kazo/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kinokawa/2026-05-12-taiyoko.md": {
	id: "kinokawa/2026-05-12-taiyoko.md";
  slug: "kinokawa/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kinokawa/2026-06-03-taishin-kaishu.md": {
	id: "kinokawa/2026-06-03-taishin-kaishu.md";
  slug: "kinokawa/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kinokawa/2026-06-05-shoene-kaishu.md": {
	id: "kinokawa/2026-06-05-shoene-kaishu.md";
  slug: "kinokawa/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kirishima/2026-05-29-taishin-kaishu.md": {
	id: "kirishima/2026-05-29-taishin-kaishu.md";
  slug: "kirishima/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kirishima/2026-06-05-shoene-kaishu.md": {
	id: "kirishima/2026-06-05-shoene-kaishu.md";
  slug: "kirishima/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kiryu/2026-06-05-shoene-kaishu.md": {
	id: "kiryu/2026-06-05-shoene-kaishu.md";
  slug: "kiryu/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kiryu/2026-06-05-taishin-kaishu.md": {
	id: "kiryu/2026-06-05-taishin-kaishu.md";
  slug: "kiryu/2026-06-05-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kishiwada/2026-05-03-marugoto-labo.md": {
	id: "kishiwada/2026-05-03-marugoto-labo.md";
  slug: "kishiwada/2026-05-03-marugoto-labo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kishiwada/2026-05-03-office-yuuchi.md": {
	id: "kishiwada/2026-05-03-office-yuuchi.md";
  slug: "kishiwada/2026-05-03-office-yuuchi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kishiwada/2026-05-16-bukkakotou-kyufukin.md": {
	id: "kishiwada/2026-05-16-bukkakotou-kyufukin.md";
  slug: "kishiwada/2026-05-16-bukkakotou-kyufukin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kishiwada/2026-05-16-ganbaru-kishiwada-hojokin.md": {
	id: "kishiwada/2026-05-16-ganbaru-kishiwada-hojokin.md";
  slug: "kishiwada/2026-05-16-ganbaru-kishiwada-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kishiwada/2026-05-16-sapoto-yushi-rishi-hosho.md": {
	id: "kishiwada/2026-05-16-sapoto-yushi-rishi-hosho.md";
  slug: "kishiwada/2026-05-16-sapoto-yushi-rishi-hosho";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kishiwada/2026-05-29-taishin-kaishu.md": {
	id: "kishiwada/2026-05-29-taishin-kaishu.md";
  slug: "kishiwada/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kishiwada/2026-06-05-shoene-kaishu.md": {
	id: "kishiwada/2026-06-05-shoene-kaishu.md";
  slug: "kishiwada/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kita_ku/2026-05-05-saiene-josei.md": {
	id: "kita_ku/2026-05-05-saiene-josei.md";
  slug: "kita_ku/2026-05-05-saiene-josei";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kita_ku/2026-05-29-taishin-kaishu.md": {
	id: "kita_ku/2026-05-29-taishin-kaishu.md";
  slug: "kita_ku/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kita_ku/2026-06-05-shoene-kaishu.md": {
	id: "kita_ku/2026-06-05-shoene-kaishu.md";
  slug: "kita_ku/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kitakami/2026-05-29-taishin-kaishu.md": {
	id: "kitakami/2026-05-29-taishin-kaishu.md";
  slug: "kitakami/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kitakami/2026-06-05-shoene-kaishu.md": {
	id: "kitakami/2026-06-05-shoene-kaishu.md";
  slug: "kitakami/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kitakyushu/2026-05-24-shokuba-yushi.md": {
	id: "kitakyushu/2026-05-24-shokuba-yushi.md";
  slug: "kitakyushu/2026-05-24-shokuba-yushi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kitakyushu/2026-05-24-taishin-shoene.md": {
	id: "kitakyushu/2026-05-24-taishin-shoene.md";
  slug: "kitakyushu/2026-05-24-taishin-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kitakyushu/2026-05-25-ijuu-shienkin.md": {
	id: "kitakyushu/2026-05-25-ijuu-shienkin.md";
  slug: "kitakyushu/2026-05-25-ijuu-shienkin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kitamoto/2026-05-10-shoene.md": {
	id: "kitamoto/2026-05-10-shoene.md";
  slug: "kitamoto/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kitamoto/2026-05-10-taiyoko.md": {
	id: "kitamoto/2026-05-10-taiyoko.md";
  slug: "kitamoto/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kitamoto/2026-06-03-taishin-kaishu.md": {
	id: "kitamoto/2026-06-03-taishin-kaishu.md";
  slug: "kitamoto/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kitanagoya/2026-05-10-jutaku-shoene.md": {
	id: "kitanagoya/2026-05-10-jutaku-shoene.md";
  slug: "kitanagoya/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kitanagoya/2026-05-10-taishin.md": {
	id: "kitanagoya/2026-05-10-taishin.md";
  slug: "kitanagoya/2026-05-10-taishin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kiyokawa/2026-05-08-chikyu-ondanka-hojo.md": {
	id: "kiyokawa/2026-05-08-chikyu-ondanka-hojo.md";
  slug: "kiyokawa/2026-05-08-chikyu-ondanka-hojo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kiyokawa/2026-05-12-iju.md": {
	id: "kiyokawa/2026-05-12-iju.md";
  slug: "kiyokawa/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kiyokawa/2026-06-03-taishin-kaishu.md": {
	id: "kiyokawa/2026-06-03-taishin-kaishu.md";
  slug: "kiyokawa/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kiyokawa/2026-06-05-shoene-kaishu.md": {
	id: "kiyokawa/2026-06-05-shoene-kaishu.md";
  slug: "kiyokawa/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kiyose/2026-05-12-taiyoko.md": {
	id: "kiyose/2026-05-12-taiyoko.md";
  slug: "kiyose/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kiyose/2026-06-03-taishin-kaishu.md": {
	id: "kiyose/2026-06-03-taishin-kaishu.md";
  slug: "kiyose/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kiyose/2026-06-05-shoene-kaishu.md": {
	id: "kiyose/2026-06-05-shoene-kaishu.md";
  slug: "kiyose/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kiyosu/2026-05-10-jutaku-reform.md": {
	id: "kiyosu/2026-05-10-jutaku-reform.md";
  slug: "kiyosu/2026-05-10-jutaku-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kiyosu/2026-05-10-jutaku-shoene.md": {
	id: "kiyosu/2026-05-10-jutaku-shoene.md";
  slug: "kiyosu/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kiyosu/2026-06-03-taishin-kaishu.md": {
	id: "kiyosu/2026-06-03-taishin-kaishu.md";
  slug: "kiyosu/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kizugawa/2026-05-11-iju.md": {
	id: "kizugawa/2026-05-11-iju.md";
  slug: "kizugawa/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kizugawa/2026-05-11-taiyoko.md": {
	id: "kizugawa/2026-05-11-taiyoko.md";
  slug: "kizugawa/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kizugawa/2026-05-29-taishin-kaishu.md": {
	id: "kizugawa/2026-05-29-taishin-kaishu.md";
  slug: "kizugawa/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kizugawa/2026-06-05-shoene-kaishu.md": {
	id: "kizugawa/2026-06-05-shoene-kaishu.md";
  slug: "kizugawa/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kobe/2026-05-11-sogyo-shien.md": {
	id: "kobe/2026-05-11-sogyo-shien.md";
  slug: "kobe/2026-05-11-sogyo-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kobe/2026-05-11-taiyoko-chikudenchi.md": {
	id: "kobe/2026-05-11-taiyoko-chikudenchi.md";
  slug: "kobe/2026-05-11-taiyoko-chikudenchi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kobe/2026-05-19-kobe-asbestos.md": {
	id: "kobe/2026-05-19-kobe-asbestos.md";
  slug: "kobe/2026-05-19-kobe-asbestos";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kobe/2026-05-19-kobe-brand-hojokin.md": {
	id: "kobe/2026-05-19-kobe-brand-hojokin.md";
  slug: "kobe/2026-05-19-kobe-brand-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kobe/2026-05-19-mansion-kanri-hojokin.md": {
	id: "kobe/2026-05-19-mansion-kanri-hojokin.md";
  slug: "kobe/2026-05-19-mansion-kanri-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kobe/2026-05-19-mansion-taishin-2026.md": {
	id: "kobe/2026-05-19-mansion-taishin-2026.md";
  slug: "kobe/2026-05-19-mansion-taishin-2026";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kobe/2026-05-19-roukyu-akiya-kaitai.md": {
	id: "kobe/2026-05-19-roukyu-akiya-kaitai.md";
  slug: "kobe/2026-05-19-roukyu-akiya-kaitai";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kobe/2026-05-19-sentan-seizogyo.md": {
	id: "kobe/2026-05-19-sentan-seizogyo.md";
  slug: "kobe/2026-05-19-sentan-seizogyo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kobe/2026-05-19-smart-nougyou.md": {
	id: "kobe/2026-05-19-smart-nougyou.md";
  slug: "kobe/2026-05-19-smart-nougyou";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kobe/2026-05-19-taishin-kaishuu-koji.md": {
	id: "kobe/2026-05-19-taishin-kaishuu-koji.md";
  slug: "kobe/2026-05-19-taishin-kaishuu-koji";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kobe/2026-05-25-kosodate-sumikaeru.md": {
	id: "kobe/2026-05-25-kosodate-sumikaeru.md";
  slug: "kobe/2026-05-25-kosodate-sumikaeru";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kobe/2026-06-05-shoene-2026.md": {
	id: "kobe/2026-06-05-shoene-2026.md";
  slug: "kobe/2026-06-05-shoene-2026";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kobe/2026-06-05-shoene-kaishu.md": {
	id: "kobe/2026-06-05-shoene-kaishu.md";
  slug: "kobe/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kochi/2026-05-27-taishin-kaishu.md": {
	id: "kochi/2026-05-27-taishin-kaishu.md";
  slug: "kochi/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kochi/2026-06-05-shoene-kaishu.md": {
	id: "kochi/2026-06-05-shoene-kaishu.md";
  slug: "kochi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kochi/2026-06-05-shoene-tax-gensaku.md": {
	id: "kochi/2026-06-05-shoene-tax-gensaku.md";
  slug: "kochi/2026-06-05-shoene-tax-gensaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kochi_pref/2026-05-27-iju-shienkin.md": {
	id: "kochi_pref/2026-05-27-iju-shienkin.md";
  slug: "kochi_pref/2026-05-27-iju-shienkin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kochi_pref/2026-05-27-taishin-kaishu.md": {
	id: "kochi_pref/2026-05-27-taishin-kaishu.md";
  slug: "kochi_pref/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kochi_pref/2026-06-05-shoene-kaishu.md": {
	id: "kochi_pref/2026-06-05-shoene-kaishu.md";
  slug: "kochi_pref/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kodaira/2026-05-12-taiyoko.md": {
	id: "kodaira/2026-05-12-taiyoko.md";
  slug: "kodaira/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kodaira/2026-06-03-taishin-kaishu.md": {
	id: "kodaira/2026-06-03-taishin-kaishu.md";
  slug: "kodaira/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kodaira/2026-06-05-shoene-kaishu.md": {
	id: "kodaira/2026-06-05-shoene-kaishu.md";
  slug: "kodaira/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kofu/2026-05-27-taishin-kaishu.md": {
	id: "kofu/2026-05-27-taishin-kaishu.md";
  slug: "kofu/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kofu/2026-06-05-clean-energy-joseikin.md": {
	id: "kofu/2026-06-05-clean-energy-joseikin.md";
  slug: "kofu/2026-06-05-clean-energy-joseikin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kofu/2026-06-05-shoene-kaishu.md": {
	id: "kofu/2026-06-05-shoene-kaishu.md";
  slug: "kofu/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koganei/2026-05-12-taiyoko.md": {
	id: "koganei/2026-05-12-taiyoko.md";
  slug: "koganei/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koganei/2026-05-29-taishin-kaishu.md": {
	id: "koganei/2026-05-29-taishin-kaishu.md";
  slug: "koganei/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koganei/2026-06-05-shoene-kaishu.md": {
	id: "koganei/2026-06-05-shoene-kaishu.md";
  slug: "koganei/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koka/2026-05-12-iju.md": {
	id: "koka/2026-05-12-iju.md";
  slug: "koka/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koka/2026-05-12-taiyoko.md": {
	id: "koka/2026-05-12-taiyoko.md";
  slug: "koka/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koka/2026-05-29-taishin-kaishu.md": {
	id: "koka/2026-05-29-taishin-kaishu.md";
  slug: "koka/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koka/2026-06-05-shoene-kaishu.md": {
	id: "koka/2026-06-05-shoene-kaishu.md";
  slug: "koka/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kokubunji/2026-05-12-taiyoko.md": {
	id: "kokubunji/2026-05-12-taiyoko.md";
  slug: "kokubunji/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kokubunji/2026-05-29-taishin-kaishu.md": {
	id: "kokubunji/2026-05-29-taishin-kaishu.md";
  slug: "kokubunji/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kokubunji/2026-06-05-shoene-kaishu.md": {
	id: "kokubunji/2026-06-05-shoene-kaishu.md";
  slug: "kokubunji/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"komae/2026-05-12-taiyoko.md": {
	id: "komae/2026-05-12-taiyoko.md";
  slug: "komae/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"komae/2026-05-29-taishin-kaishu.md": {
	id: "komae/2026-05-29-taishin-kaishu.md";
  slug: "komae/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"komae/2026-06-05-shoene-kaishu.md": {
	id: "komae/2026-06-05-shoene-kaishu.md";
  slug: "komae/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"komaki/2026-05-10-jutaku-shoene.md": {
	id: "komaki/2026-05-10-jutaku-shoene.md";
  slug: "komaki/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"komaki/2026-05-10-sogyo-shien.md": {
	id: "komaki/2026-05-10-sogyo-shien.md";
  slug: "komaki/2026-05-10-sogyo-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"komaki/2026-05-29-taishin-kaishu.md": {
	id: "komaki/2026-05-29-taishin-kaishu.md";
  slug: "komaki/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"konan_aichi/2026-05-10-jutaku-shoene.md": {
	id: "konan_aichi/2026-05-10-jutaku-shoene.md";
  slug: "konan_aichi/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"konan_aichi/2026-05-10-taishin.md": {
	id: "konan_aichi/2026-05-10-taishin.md";
  slug: "konan_aichi/2026-05-10-taishin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"konan_shiga/2026-05-12-taiyoko.md": {
	id: "konan_shiga/2026-05-12-taiyoko.md";
  slug: "konan_shiga/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"konan_shiga/2026-05-29-taishin-kaishu.md": {
	id: "konan_shiga/2026-05-29-taishin-kaishu.md";
  slug: "konan_shiga/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"konan_shiga/2026-06-05-shoene-kaishu.md": {
	id: "konan_shiga/2026-06-05-shoene-kaishu.md";
  slug: "konan_shiga/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"konosu/2026-05-10-new_build.md": {
	id: "konosu/2026-05-10-new_build.md";
  slug: "konosu/2026-05-10-new_build";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"konosu/2026-05-10-saiene.md": {
	id: "konosu/2026-05-10-saiene.md";
  slug: "konosu/2026-05-10-saiene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"konosu/2026-05-10-sub2.md": {
	id: "konosu/2026-05-10-sub2.md";
  slug: "konosu/2026-05-10-sub2";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"konosu/2026-06-03-taishin-kaishu.md": {
	id: "konosu/2026-06-03-taishin-kaishu.md";
  slug: "konosu/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"konosu/2026-06-05-shoene-kaishu.md": {
	id: "konosu/2026-06-05-shoene-kaishu.md";
  slug: "konosu/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kora/2026-05-12-taiyoko.md": {
	id: "kora/2026-05-12-taiyoko.md";
  slug: "kora/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kora/2026-05-29-taishin-kaishu.md": {
	id: "kora/2026-05-29-taishin-kaishu.md";
  slug: "kora/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kora/2026-06-05-shoene-kaishu.md": {
	id: "kora/2026-06-05-shoene-kaishu.md";
  slug: "kora/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koriyama/2026-05-29-taishin-kaishu.md": {
	id: "koriyama/2026-05-29-taishin-kaishu.md";
  slug: "koriyama/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koriyama/2026-06-05-shoene-kaishu.md": {
	id: "koriyama/2026-06-05-shoene-kaishu.md";
  slug: "koriyama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koryo/2026-05-12-taiyoko.md": {
	id: "koryo/2026-05-12-taiyoko.md";
  slug: "koryo/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koryo/2026-06-03-taishin-kaishu.md": {
	id: "koryo/2026-06-03-taishin-kaishu.md";
  slug: "koryo/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koryo/2026-06-05-shoene-kaishu.md": {
	id: "koryo/2026-06-05-shoene-kaishu.md";
  slug: "koryo/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koshigaya/2026-05-10-kaden.md": {
	id: "koshigaya/2026-05-10-kaden.md";
  slug: "koshigaya/2026-05-10-kaden";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koshigaya/2026-05-10-saiene.md": {
	id: "koshigaya/2026-05-10-saiene.md";
  slug: "koshigaya/2026-05-10-saiene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koshigaya/2026-05-10-shoene-kaden.md": {
	id: "koshigaya/2026-05-10-shoene-kaden.md";
  slug: "koshigaya/2026-05-10-shoene-kaden";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koshigaya/2026-05-29-taishin-kaishu.md": {
	id: "koshigaya/2026-05-29-taishin-kaishu.md";
  slug: "koshigaya/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kota/2026-05-10-shoene-aichi-pref.md": {
	id: "kota/2026-05-10-shoene-aichi-pref.md";
  slug: "kota/2026-05-10-shoene-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kota/2026-05-10-sogyo-aichi-pref.md": {
	id: "kota/2026-05-10-sogyo-aichi-pref.md";
  slug: "kota/2026-05-10-sogyo-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kota/2026-06-03-taishin-kaishu.md": {
	id: "kota/2026-06-03-taishin-kaishu.md";
  slug: "kota/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koto_ku/2026-05-05-ondanka-boshi.md": {
	id: "koto_ku/2026-05-05-ondanka-boshi.md";
  slug: "koto_ku/2026-05-05-ondanka-boshi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koto_ku/2026-05-29-taishin-kaishu.md": {
	id: "koto_ku/2026-05-29-taishin-kaishu.md";
  slug: "koto_ku/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koto_ku/2026-06-05-shoene-kaishu.md": {
	id: "koto_ku/2026-06-05-shoene-kaishu.md";
  slug: "koto_ku/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koya/2026-05-12-iju.md": {
	id: "koya/2026-05-12-iju.md";
  slug: "koya/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koya/2026-05-12-taiyoko.md": {
	id: "koya/2026-05-12-taiyoko.md";
  slug: "koya/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koya/2026-06-03-taishin-kaishu.md": {
	id: "koya/2026-06-03-taishin-kaishu.md";
  slug: "koya/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"koya/2026-06-05-shoene-kaishu.md": {
	id: "koya/2026-06-05-shoene-kaishu.md";
  slug: "koya/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kuki/2026-05-10-shoene.md": {
	id: "kuki/2026-05-10-shoene.md";
  slug: "kuki/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kuki/2026-05-10-taiyoko.md": {
	id: "kuki/2026-05-10-taiyoko.md";
  slug: "kuki/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kuki/2026-06-03-taishin-kaishu.md": {
	id: "kuki/2026-06-03-taishin-kaishu.md";
  slug: "kuki/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kumagaya/2026-05-10-saiene.md": {
	id: "kumagaya/2026-05-10-saiene.md";
  slug: "kumagaya/2026-05-10-saiene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kumagaya/2026-05-10-sogyo.md": {
	id: "kumagaya/2026-05-10-sogyo.md";
  slug: "kumagaya/2026-05-10-sogyo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kumagaya/2026-05-29-taishin-kaishu.md": {
	id: "kumagaya/2026-05-29-taishin-kaishu.md";
  slug: "kumagaya/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kumagaya/2026-06-05-shoene-kaishu.md": {
	id: "kumagaya/2026-06-05-shoene-kaishu.md";
  slug: "kumagaya/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kumamoto/2026-05-24-chusho-yushi.md": {
	id: "kumamoto/2026-05-24-chusho-yushi.md";
  slug: "kumamoto/2026-05-24-chusho-yushi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kumamoto/2026-05-24-shoene-kiki.md": {
	id: "kumamoto/2026-05-24-shoene-kiki.md";
  slug: "kumamoto/2026-05-24-shoene-kiki";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kumamoto/2026-05-25-ijuu-shienkin.md": {
	id: "kumamoto/2026-05-25-ijuu-shienkin.md";
  slug: "kumamoto/2026-05-25-ijuu-shienkin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kumamoto/2026-05-25-kosodate-jutaku.md": {
	id: "kumamoto/2026-05-25-kosodate-jutaku.md";
  slug: "kumamoto/2026-05-25-kosodate-jutaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kumamoto/2026-05-29-taishin-kaishu.md": {
	id: "kumamoto/2026-05-29-taishin-kaishu.md";
  slug: "kumamoto/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kumamoto_pref/2026-06-05-shoene-kaishu.md": {
	id: "kumamoto_pref/2026-06-05-shoene-kaishu.md";
  slug: "kumamoto_pref/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kumamoto_pref/2026-06-05-taishin-kaishu.md": {
	id: "kumamoto_pref/2026-06-05-taishin-kaishu.md";
  slug: "kumamoto_pref/2026-06-05-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kumatori/2026-05-04-sangyo-kasseika.md": {
	id: "kumatori/2026-05-04-sangyo-kasseika.md";
  slug: "kumatori/2026-05-04-sangyo-kasseika";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kumatori/2026-05-04-sansei-kindai.md": {
	id: "kumatori/2026-05-04-sansei-kindai.md";
  slug: "kumatori/2026-05-04-sansei-kindai";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kumatori/2026-06-03-taishin-kaishu.md": {
	id: "kumatori/2026-06-03-taishin-kaishu.md";
  slug: "kumatori/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kumatori/2026-06-05-shoene-kaishu.md": {
	id: "kumatori/2026-06-05-shoene-kaishu.md";
  slug: "kumatori/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kumiyama/2026-05-11-taiyoko.md": {
	id: "kumiyama/2026-05-11-taiyoko.md";
  slug: "kumiyama/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kumiyama/2026-05-29-taishin-kaishu.md": {
	id: "kumiyama/2026-05-29-taishin-kaishu.md";
  slug: "kumiyama/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kumiyama/2026-06-05-shoene-kaishu.md": {
	id: "kumiyama/2026-06-05-shoene-kaishu.md";
  slug: "kumiyama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kunitachi/2026-05-12-taiyoko.md": {
	id: "kunitachi/2026-05-12-taiyoko.md";
  slug: "kunitachi/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kunitachi/2026-05-29-taishin-kaishu.md": {
	id: "kunitachi/2026-05-29-taishin-kaishu.md";
  slug: "kunitachi/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kunitachi/2026-06-05-shoene-kaishu.md": {
	id: "kunitachi/2026-06-05-shoene-kaishu.md";
  slug: "kunitachi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kurashiki/2026-05-29-taishin-kaishu.md": {
	id: "kurashiki/2026-05-29-taishin-kaishu.md";
  slug: "kurashiki/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kurashiki/2026-06-05-shoene-kaishu.md": {
	id: "kurashiki/2026-06-05-shoene-kaishu.md";
  slug: "kurashiki/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kure/2026-05-29-taishin-kaishu.md": {
	id: "kure/2026-05-29-taishin-kaishu.md";
  slug: "kure/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kure/2026-06-05-shoene-kaishu.md": {
	id: "kure/2026-06-05-shoene-kaishu.md";
  slug: "kure/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kurume/2026-05-29-taishin-kaishu.md": {
	id: "kurume/2026-05-29-taishin-kaishu.md";
  slug: "kurume/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kurume/2026-06-05-shoene-kaishu.md": {
	id: "kurume/2026-06-05-shoene-kaishu.md";
  slug: "kurume/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kusatsu/2026-05-12-kosodate.md": {
	id: "kusatsu/2026-05-12-kosodate.md";
  slug: "kusatsu/2026-05-12-kosodate";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kusatsu/2026-05-12-taiyoko.md": {
	id: "kusatsu/2026-05-12-taiyoko.md";
  slug: "kusatsu/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kusatsu/2026-05-29-taishin-kaishu.md": {
	id: "kusatsu/2026-05-29-taishin-kaishu.md";
  slug: "kusatsu/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kusatsu/2026-06-05-shoene-kaishu.md": {
	id: "kusatsu/2026-06-05-shoene-kaishu.md";
  slug: "kusatsu/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kuwana/2026-05-29-taishin-kaishu.md": {
	id: "kuwana/2026-05-29-taishin-kaishu.md";
  slug: "kuwana/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kuwana/2026-06-05-shoene-kaishu.md": {
	id: "kuwana/2026-06-05-shoene-kaishu.md";
  slug: "kuwana/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyotamba/2026-05-11-iju.md": {
	id: "kyotamba/2026-05-11-iju.md";
  slug: "kyotamba/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyotamba/2026-05-11-taiyoko.md": {
	id: "kyotamba/2026-05-11-taiyoko.md";
  slug: "kyotamba/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyotamba/2026-06-03-taishin-kaishu.md": {
	id: "kyotamba/2026-06-03-taishin-kaishu.md";
  slug: "kyotamba/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyotamba/2026-06-05-shoene-kaishu.md": {
	id: "kyotamba/2026-06-05-shoene-kaishu.md";
  slug: "kyotamba/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyotanabe/2026-05-11-taiyoko.md": {
	id: "kyotanabe/2026-05-11-taiyoko.md";
  slug: "kyotanabe/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyotanabe/2026-05-29-taishin-kaishu.md": {
	id: "kyotanabe/2026-05-29-taishin-kaishu.md";
  slug: "kyotanabe/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyotanabe/2026-06-05-shoene-kaishu.md": {
	id: "kyotanabe/2026-06-05-shoene-kaishu.md";
  slug: "kyotanabe/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyotango/2026-05-11-iju.md": {
	id: "kyotango/2026-05-11-iju.md";
  slug: "kyotango/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyotango/2026-05-11-taiyoko.md": {
	id: "kyotango/2026-05-11-taiyoko.md";
  slug: "kyotango/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyotango/2026-05-29-taishin-kaishu.md": {
	id: "kyotango/2026-05-29-taishin-kaishu.md";
  slug: "kyotango/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyotango/2026-06-05-shoene-kaishu.md": {
	id: "kyotango/2026-06-05-shoene-kaishu.md";
  slug: "kyotango/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyoto/2026-05-11-kosodatesumai.md": {
	id: "kyoto/2026-05-11-kosodatesumai.md";
  slug: "kyoto/2026-05-11-kosodatesumai";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyoto/2026-05-11-taiyoko.md": {
	id: "kyoto/2026-05-11-taiyoko.md";
  slug: "kyoto/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyoto/2026-05-20-chuusho-yushi.md": {
	id: "kyoto/2026-05-20-chuusho-yushi.md";
  slug: "kyoto/2026-05-20-chuusho-yushi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyoto/2026-05-20-sentansetubi-nintei.md": {
	id: "kyoto/2026-05-20-sentansetubi-nintei.md";
  slug: "kyoto/2026-05-20-sentansetubi-nintei";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyoto/2026-05-27-taishin-kaishu.md": {
	id: "kyoto/2026-05-27-taishin-kaishu.md";
  slug: "kyoto/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyoto/2026-06-05-shoene-kaishu.md": {
	id: "kyoto/2026-06-05-shoene-kaishu.md";
  slug: "kyoto/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyoto_pref/2026-05-11-iju-shien.md": {
	id: "kyoto_pref/2026-05-11-iju-shien.md";
  slug: "kyoto_pref/2026-05-11-iju-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyoto_pref/2026-05-11-sogyo.md": {
	id: "kyoto_pref/2026-05-11-sogyo.md";
  slug: "kyoto_pref/2026-05-11-sogyo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyoto_pref/2026-05-11-taiyoko-chikudenchi.md": {
	id: "kyoto_pref/2026-05-11-taiyoko-chikudenchi.md";
  slug: "kyoto_pref/2026-05-11-taiyoko-chikudenchi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyoto_pref/2026-05-17-jgrants-cdylxmah.md": {
	id: "kyoto_pref/2026-05-17-jgrants-cdylxmah.md";
  slug: "kyoto_pref/2026-05-17-jgrants-cdylxmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyoto_pref/2026-06-03-shoene-kaishu.md": {
	id: "kyoto_pref/2026-06-03-shoene-kaishu.md";
  slug: "kyoto_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"kyoto_pref/2026-06-03-taishin-kaishu.md": {
	id: "kyoto_pref/2026-06-03-taishin-kaishu.md";
  slug: "kyoto_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"machida/2026-05-06-enefarm.md": {
	id: "machida/2026-05-06-enefarm.md";
  slug: "machida/2026-05-06-enefarm";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"machida/2026-05-29-taishin-kaishu.md": {
	id: "machida/2026-05-29-taishin-kaishu.md";
  slug: "machida/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"machida/2026-06-05-shoene-kaishu.md": {
	id: "machida/2026-06-05-shoene-kaishu.md";
  slug: "machida/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"maebashi/2026-05-27-iju-shienkin.md": {
	id: "maebashi/2026-05-27-iju-shienkin.md";
  slug: "maebashi/2026-05-27-iju-shienkin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"maebashi/2026-05-27-taiyoko.md": {
	id: "maebashi/2026-05-27-taiyoko.md";
  slug: "maebashi/2026-05-27-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"maebashi/2026-05-29-taishin-kaishu.md": {
	id: "maebashi/2026-05-29-taishin-kaishu.md";
  slug: "maebashi/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"maebashi/2026-06-05-shoene-kaishu.md": {
	id: "maebashi/2026-06-05-shoene-kaishu.md";
  slug: "maebashi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"maibara/2026-05-12-iju.md": {
	id: "maibara/2026-05-12-iju.md";
  slug: "maibara/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"maibara/2026-05-12-taiyoko.md": {
	id: "maibara/2026-05-12-taiyoko.md";
  slug: "maibara/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"maibara/2026-05-29-taishin-kaishu.md": {
	id: "maibara/2026-05-29-taishin-kaishu.md";
  slug: "maibara/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"maibara/2026-06-05-shoene-kaishu.md": {
	id: "maibara/2026-06-05-shoene-kaishu.md";
  slug: "maibara/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"maizuru/2026-05-11-iju.md": {
	id: "maizuru/2026-05-11-iju.md";
  slug: "maizuru/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"maizuru/2026-05-11-taiyoko.md": {
	id: "maizuru/2026-05-11-taiyoko.md";
  slug: "maizuru/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"maizuru/2026-05-29-taishin-kaishu.md": {
	id: "maizuru/2026-05-29-taishin-kaishu.md";
  slug: "maizuru/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"maizuru/2026-06-05-shoene-kaishu.md": {
	id: "maizuru/2026-06-05-shoene-kaishu.md";
  slug: "maizuru/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"manazuru/2026-05-08-taiyoko-hojo.md": {
	id: "manazuru/2026-05-08-taiyoko-hojo.md";
  slug: "manazuru/2026-05-08-taiyoko-hojo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"manazuru/2026-06-03-taishin-kaishu.md": {
	id: "manazuru/2026-06-03-taishin-kaishu.md";
  slug: "manazuru/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"manazuru/2026-06-05-shoene-kaishu.md": {
	id: "manazuru/2026-06-05-shoene-kaishu.md";
  slug: "manazuru/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"marugame/2026-05-29-taishin-kaishu.md": {
	id: "marugame/2026-05-29-taishin-kaishu.md";
  slug: "marugame/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"marugame/2026-06-05-shoene-kaishu.md": {
	id: "marugame/2026-06-05-shoene-kaishu.md";
  slug: "marugame/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsubara/2026-05-01-akiya-jokyaku.md": {
	id: "matsubara/2026-05-01-akiya-jokyaku.md";
  slug: "matsubara/2026-05-01-akiya-jokyaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsubara/2026-05-01-akiya-sogyo.md": {
	id: "matsubara/2026-05-01-akiya-sogyo.md";
  slug: "matsubara/2026-05-01-akiya-sogyo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsubara/2026-05-01-shogakukin.md": {
	id: "matsubara/2026-05-01-shogakukin.md";
  slug: "matsubara/2026-05-01-shogakukin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsubara/2026-05-29-taishin-kaishu.md": {
	id: "matsubara/2026-05-29-taishin-kaishu.md";
  slug: "matsubara/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsubara/2026-06-05-shoene-kaishu.md": {
	id: "matsubara/2026-06-05-shoene-kaishu.md";
  slug: "matsubara/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsubushi/2026-05-10-shoene.md": {
	id: "matsubushi/2026-05-10-shoene.md";
  slug: "matsubushi/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsubushi/2026-05-10-taiyoko.md": {
	id: "matsubushi/2026-05-10-taiyoko.md";
  slug: "matsubushi/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsubushi/2026-06-03-taishin-kaishu.md": {
	id: "matsubushi/2026-06-03-taishin-kaishu.md";
  slug: "matsubushi/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsuda/2026-05-08-smart-house.md": {
	id: "matsuda/2026-05-08-smart-house.md";
  slug: "matsuda/2026-05-08-smart-house";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsuda/2026-06-03-taishin-kaishu.md": {
	id: "matsuda/2026-06-03-taishin-kaishu.md";
  slug: "matsuda/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsuda/2026-06-05-shoene-kaishu.md": {
	id: "matsuda/2026-06-05-shoene-kaishu.md";
  slug: "matsuda/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsudo/2026-05-22-jgrants-cdzifma5.md": {
	id: "matsudo/2026-05-22-jgrants-cdzifma5.md";
  slug: "matsudo/2026-05-22-jgrants-cdzifma5";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsudo/2026-05-29-taishin-kaishu.md": {
	id: "matsudo/2026-05-29-taishin-kaishu.md";
  slug: "matsudo/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsudo/2026-06-05-shoene-kaishu.md": {
	id: "matsudo/2026-06-05-shoene-kaishu.md";
  slug: "matsudo/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsue/2026-05-27-akiya-reform.md": {
	id: "matsue/2026-05-27-akiya-reform.md";
  slug: "matsue/2026-05-27-akiya-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsue/2026-05-29-taishin-kaishu.md": {
	id: "matsue/2026-05-29-taishin-kaishu.md";
  slug: "matsue/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsue/2026-06-05-saiene-hojokin.md": {
	id: "matsue/2026-06-05-saiene-hojokin.md";
  slug: "matsue/2026-06-05-saiene-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsue/2026-06-05-shoene-kaishu.md": {
	id: "matsue/2026-06-05-shoene-kaishu.md";
  slug: "matsue/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsumoto/2026-05-29-taishin-kaishu.md": {
	id: "matsumoto/2026-05-29-taishin-kaishu.md";
  slug: "matsumoto/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsumoto/2026-06-05-shoene-kaishu.md": {
	id: "matsumoto/2026-06-05-shoene-kaishu.md";
  slug: "matsumoto/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsumoto/2026-06-05-zero-carbon-hojo.md": {
	id: "matsumoto/2026-06-05-zero-carbon-hojo.md";
  slug: "matsumoto/2026-06-05-zero-carbon-hojo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsuyama/2026-05-27-reform.md": {
	id: "matsuyama/2026-05-27-reform.md";
  slug: "matsuyama/2026-05-27-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsuyama/2026-05-27-taishin-kaishu.md": {
	id: "matsuyama/2026-05-27-taishin-kaishu.md";
  slug: "matsuyama/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"matsuyama/2026-06-05-shoene-kaishu.md": {
	id: "matsuyama/2026-06-05-shoene-kaishu.md";
  slug: "matsuyama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"meguro/2026-05-05-saiene-josei.md": {
	id: "meguro/2026-05-05-saiene-josei.md";
  slug: "meguro/2026-05-05-saiene-josei";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"meguro/2026-05-29-taishin-kaishu.md": {
	id: "meguro/2026-05-29-taishin-kaishu.md";
  slug: "meguro/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"meguro/2026-06-05-shoene-kaishu.md": {
	id: "meguro/2026-06-05-shoene-kaishu.md";
  slug: "meguro/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"mie_pref/2026-05-17-jgrants-cdy44mah.md": {
	id: "mie_pref/2026-05-17-jgrants-cdy44mah.md";
  slug: "mie_pref/2026-05-17-jgrants-cdy44mah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"mie_pref/2026-05-17-jgrants-cdyssmax.md": {
	id: "mie_pref/2026-05-17-jgrants-cdyssmax.md";
  slug: "mie_pref/2026-05-17-jgrants-cdyssmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"mie_pref/2026-06-03-shoene-kaishu.md": {
	id: "mie_pref/2026-06-03-shoene-kaishu.md";
  slug: "mie_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"mie_pref/2026-06-03-taishin-kaishu.md": {
	id: "mie_pref/2026-06-03-taishin-kaishu.md";
  slug: "mie_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"mihama_aichi/2026-05-10-shoene-aichi-pref.md": {
	id: "mihama_aichi/2026-05-10-shoene-aichi-pref.md";
  slug: "mihama_aichi/2026-05-10-shoene-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"mihama_aichi/2026-05-10-sogyo-aichi-pref.md": {
	id: "mihama_aichi/2026-05-10-sogyo-aichi-pref.md";
  slug: "mihama_aichi/2026-05-10-sogyo-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"mihama_aichi/2026-06-03-taishin-kaishu.md": {
	id: "mihama_aichi/2026-06-03-taishin-kaishu.md";
  slug: "mihama_aichi/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"mikata_h/2026-05-11-iju.md": {
	id: "mikata_h/2026-05-11-iju.md";
  slug: "mikata_h/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"mikata_h/2026-05-11-taiyoko.md": {
	id: "mikata_h/2026-05-11-taiyoko.md";
  slug: "mikata_h/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"mikata_h/2026-06-03-taishin-kaishu.md": {
	id: "mikata_h/2026-06-03-taishin-kaishu.md";
  slug: "mikata_h/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"mikata_h/2026-06-05-shoene-kaishu.md": {
	id: "mikata_h/2026-06-05-shoene-kaishu.md";
  slug: "mikata_h/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miki/2026-05-11-akiya.md": {
	id: "miki/2026-05-11-akiya.md";
  slug: "miki/2026-05-11-akiya";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miki/2026-05-11-taiyoko.md": {
	id: "miki/2026-05-11-taiyoko.md";
  slug: "miki/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miki/2026-05-29-taishin-kaishu.md": {
	id: "miki/2026-05-29-taishin-kaishu.md";
  slug: "miki/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miki/2026-06-05-shoene-kaishu.md": {
	id: "miki/2026-06-05-shoene-kaishu.md";
  slug: "miki/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minamiashigara/2026-05-08-saiene-hojo.md": {
	id: "minamiashigara/2026-05-08-saiene-hojo.md";
  slug: "minamiashigara/2026-05-08-saiene-hojo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minamiashigara/2026-05-12-iju.md": {
	id: "minamiashigara/2026-05-12-iju.md";
  slug: "minamiashigara/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minamiashigara/2026-06-03-taishin-kaishu.md": {
	id: "minamiashigara/2026-06-03-taishin-kaishu.md";
  slug: "minamiashigara/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minamiashigara/2026-06-05-shoene-kaishu.md": {
	id: "minamiashigara/2026-06-05-shoene-kaishu.md";
  slug: "minamiashigara/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minamiawaji/2026-05-11-iju.md": {
	id: "minamiawaji/2026-05-11-iju.md";
  slug: "minamiawaji/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minamiawaji/2026-05-11-taiyoko.md": {
	id: "minamiawaji/2026-05-11-taiyoko.md";
  slug: "minamiawaji/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minamiawaji/2026-05-29-taishin-kaishu.md": {
	id: "minamiawaji/2026-05-29-taishin-kaishu.md";
  slug: "minamiawaji/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minamiawaji/2026-06-05-shoene-kaishu.md": {
	id: "minamiawaji/2026-06-05-shoene-kaishu.md";
  slug: "minamiawaji/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minamichita/2026-05-10-shoene-aichi-pref.md": {
	id: "minamichita/2026-05-10-shoene-aichi-pref.md";
  slug: "minamichita/2026-05-10-shoene-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minamichita/2026-05-10-sogyo-aichi-pref.md": {
	id: "minamichita/2026-05-10-sogyo-aichi-pref.md";
  slug: "minamichita/2026-05-10-sogyo-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minamichita/2026-06-03-taishin-kaishu.md": {
	id: "minamichita/2026-06-03-taishin-kaishu.md";
  slug: "minamichita/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minamiyamashiro/2026-05-11-iju.md": {
	id: "minamiyamashiro/2026-05-11-iju.md";
  slug: "minamiyamashiro/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minamiyamashiro/2026-05-11-taiyoko.md": {
	id: "minamiyamashiro/2026-05-11-taiyoko.md";
  slug: "minamiyamashiro/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minamiyamashiro/2026-06-03-taishin-kaishu.md": {
	id: "minamiyamashiro/2026-06-03-taishin-kaishu.md";
  slug: "minamiyamashiro/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minamiyamashiro/2026-06-05-shoene-kaishu.md": {
	id: "minamiyamashiro/2026-06-05-shoene-kaishu.md";
  slug: "minamiyamashiro/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minano/2026-05-10-shoene.md": {
	id: "minano/2026-05-10-shoene.md";
  slug: "minano/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minano/2026-05-10-taiyoko.md": {
	id: "minano/2026-05-10-taiyoko.md";
  slug: "minano/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minano/2026-06-03-taishin-kaishu.md": {
	id: "minano/2026-06-03-taishin-kaishu.md";
  slug: "minano/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minato_ku/2026-05-05-jutaku-shutoku.md": {
	id: "minato_ku/2026-05-05-jutaku-shutoku.md";
  slug: "minato_ku/2026-05-05-jutaku-shutoku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minato_ku/2026-05-29-taishin-kaishu.md": {
	id: "minato_ku/2026-05-29-taishin-kaishu.md";
  slug: "minato_ku/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minato_ku/2026-06-05-shoene-kaishu.md": {
	id: "minato_ku/2026-06-05-shoene-kaishu.md";
  slug: "minato_ku/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minoh/2026-05-04-shoko-hojokin.md": {
	id: "minoh/2026-05-04-shoko-hojokin.md";
  slug: "minoh/2026-05-04-shoko-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minoh/2026-05-04-taishin.md": {
	id: "minoh/2026-05-04-taishin.md";
  slug: "minoh/2026-05-04-taishin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"minoh/2026-06-05-shoene-kaishu.md": {
	id: "minoh/2026-06-05-shoene-kaishu.md";
  slug: "minoh/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"misaki/2026-05-04-akiya-saisei.md": {
	id: "misaki/2026-05-04-akiya-saisei.md";
  slug: "misaki/2026-05-04-akiya-saisei";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"misaki/2026-05-04-chuko-jutaku.md": {
	id: "misaki/2026-05-04-chuko-jutaku.md";
  slug: "misaki/2026-05-04-chuko-jutaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"misaki/2026-06-03-taishin-kaishu.md": {
	id: "misaki/2026-06-03-taishin-kaishu.md";
  slug: "misaki/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"misaki/2026-06-05-shoene-kaishu.md": {
	id: "misaki/2026-06-05-shoene-kaishu.md";
  slug: "misaki/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"misato/2026-05-10-shoene.md": {
	id: "misato/2026-05-10-shoene.md";
  slug: "misato/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"misato/2026-05-10-taiyoko.md": {
	id: "misato/2026-05-10-taiyoko.md";
  slug: "misato/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"misato/2026-06-03-taishin-kaishu.md": {
	id: "misato/2026-06-03-taishin-kaishu.md";
  slug: "misato/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"misato_machi/2026-05-10-shoene.md": {
	id: "misato_machi/2026-05-10-shoene.md";
  slug: "misato_machi/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"misato_machi/2026-05-10-taiyoko.md": {
	id: "misato_machi/2026-05-10-taiyoko.md";
  slug: "misato_machi/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"misato_machi/2026-06-03-taishin-kaishu.md": {
	id: "misato_machi/2026-06-03-taishin-kaishu.md";
  slug: "misato_machi/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"mitaka/2026-05-06-shoene-josei.md": {
	id: "mitaka/2026-05-06-shoene-josei.md";
  slug: "mitaka/2026-05-06-shoene-josei";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"mitaka/2026-05-29-taishin-kaishu.md": {
	id: "mitaka/2026-05-29-taishin-kaishu.md";
  slug: "mitaka/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"mito/2026-05-27-taishin-kaishu.md": {
	id: "mito/2026-05-27-taishin-kaishu.md";
  slug: "mito/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"mito/2026-06-05-shoene-hojo.md": {
	id: "mito/2026-06-05-shoene-hojo.md";
  slug: "mito/2026-06-05-shoene-hojo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"mito/2026-06-05-shoene-kaishu.md": {
	id: "mito/2026-06-05-shoene-kaishu.md";
  slug: "mito/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miura/2026-05-08-juuten-taiyoko.md": {
	id: "miura/2026-05-08-juuten-taiyoko.md";
  slug: "miura/2026-05-08-juuten-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miura/2026-05-12-iju.md": {
	id: "miura/2026-05-12-iju.md";
  slug: "miura/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miura/2026-06-03-taishin-kaishu.md": {
	id: "miura/2026-06-03-taishin-kaishu.md";
  slug: "miura/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miura/2026-06-05-shoene-kaishu.md": {
	id: "miura/2026-06-05-shoene-kaishu.md";
  slug: "miura/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyagi_pref/2026-05-17-jgrants-cdzcomah.md": {
	id: "miyagi_pref/2026-05-17-jgrants-cdzcomah.md";
  slug: "miyagi_pref/2026-05-17-jgrants-cdzcomah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyagi_pref/2026-06-03-shoene-kaishu.md": {
	id: "miyagi_pref/2026-06-03-shoene-kaishu.md";
  slug: "miyagi_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyagi_pref/2026-06-03-taishin-kaishu.md": {
	id: "miyagi_pref/2026-06-03-taishin-kaishu.md";
  slug: "miyagi_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyakonojo/2026-06-05-shoene-kaishu.md": {
	id: "miyakonojo/2026-06-05-shoene-kaishu.md";
  slug: "miyakonojo/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyakonojo/2026-06-05-taishin-kaishu.md": {
	id: "miyakonojo/2026-06-05-taishin-kaishu.md";
  slug: "miyakonojo/2026-06-05-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyashiro/2026-05-10-shoene.md": {
	id: "miyashiro/2026-05-10-shoene.md";
  slug: "miyashiro/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyashiro/2026-05-10-taiyoko.md": {
	id: "miyashiro/2026-05-10-taiyoko.md";
  slug: "miyashiro/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyashiro/2026-06-03-taishin-kaishu.md": {
	id: "miyashiro/2026-06-03-taishin-kaishu.md";
  slug: "miyashiro/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyazaki/2026-05-27-taishin-kaishu.md": {
	id: "miyazaki/2026-05-27-taishin-kaishu.md";
  slug: "miyazaki/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyazaki/2026-06-05-shoene-kaishu.md": {
	id: "miyazaki/2026-06-05-shoene-kaishu.md";
  slug: "miyazaki/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyazaki/2026-06-05-shoene-kenshu.md": {
	id: "miyazaki/2026-06-05-shoene-kenshu.md";
  slug: "miyazaki/2026-06-05-shoene-kenshu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyazaki_pref/2026-05-17-jgrants-cdvytmax.md": {
	id: "miyazaki_pref/2026-05-17-jgrants-cdvytmax.md";
  slug: "miyazaki_pref/2026-05-17-jgrants-cdvytmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyazaki_pref/2026-06-03-shoene-kaishu.md": {
	id: "miyazaki_pref/2026-06-03-shoene-kaishu.md";
  slug: "miyazaki_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyazaki_pref/2026-06-03-taishin-kaishu.md": {
	id: "miyazaki_pref/2026-06-03-taishin-kaishu.md";
  slug: "miyazaki_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyazu/2026-05-11-iju.md": {
	id: "miyazu/2026-05-11-iju.md";
  slug: "miyazu/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyazu/2026-05-11-taiyoko.md": {
	id: "miyazu/2026-05-11-taiyoko.md";
  slug: "miyazu/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyazu/2026-05-29-taishin-kaishu.md": {
	id: "miyazu/2026-05-29-taishin-kaishu.md";
  slug: "miyazu/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyazu/2026-06-05-shoene-kaishu.md": {
	id: "miyazu/2026-06-05-shoene-kaishu.md";
  slug: "miyazu/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyoshi/2026-05-10-jutaku-shoene.md": {
	id: "miyoshi/2026-05-10-jutaku-shoene.md";
  slug: "miyoshi/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyoshi/2026-05-10-taishin.md": {
	id: "miyoshi/2026-05-10-taishin.md";
  slug: "miyoshi/2026-05-10-taishin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyoshi/2026-05-29-taishin-kaishu.md": {
	id: "miyoshi/2026-05-29-taishin-kaishu.md";
  slug: "miyoshi/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyoshi_machi/2026-05-10-shoene.md": {
	id: "miyoshi_machi/2026-05-10-shoene.md";
  slug: "miyoshi_machi/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyoshi_machi/2026-05-10-taiyoko.md": {
	id: "miyoshi_machi/2026-05-10-taiyoko.md";
  slug: "miyoshi_machi/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"miyoshi_machi/2026-06-03-taishin-kaishu.md": {
	id: "miyoshi_machi/2026-06-03-taishin-kaishu.md";
  slug: "miyoshi_machi/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"mizuho/2026-05-12-taiyoko.md": {
	id: "mizuho/2026-05-12-taiyoko.md";
  slug: "mizuho/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"mizuho/2026-06-03-taishin-kaishu.md": {
	id: "mizuho/2026-06-03-taishin-kaishu.md";
  slug: "mizuho/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"mizuho/2026-06-05-shoene-kaishu.md": {
	id: "mizuho/2026-06-05-shoene-kaishu.md";
  slug: "mizuho/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"moriguchi/2026-05-01-kogyo-kasseika.md": {
	id: "moriguchi/2026-05-01-kogyo-kasseika.md";
  slug: "moriguchi/2026-05-01-kogyo-kasseika";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"moriguchi/2026-05-01-shogyo-shinko.md": {
	id: "moriguchi/2026-05-01-shogyo-shinko.md";
  slug: "moriguchi/2026-05-01-shogyo-shinko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"moriguchi/2026-05-29-taishin-kaishu.md": {
	id: "moriguchi/2026-05-29-taishin-kaishu.md";
  slug: "moriguchi/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"moriguchi/2026-06-05-shoene-kaishu.md": {
	id: "moriguchi/2026-06-05-shoene-kaishu.md";
  slug: "moriguchi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"morioka/2026-05-27-iju-shienkin.md": {
	id: "morioka/2026-05-27-iju-shienkin.md";
  slug: "morioka/2026-05-27-iju-shienkin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"morioka/2026-05-27-shoene-reform.md": {
	id: "morioka/2026-05-27-shoene-reform.md";
  slug: "morioka/2026-05-27-shoene-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"morioka/2026-05-29-taishin-kaishu.md": {
	id: "morioka/2026-05-29-taishin-kaishu.md";
  slug: "morioka/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"morioka/2026-06-05-shoene-kaishu.md": {
	id: "morioka/2026-06-05-shoene-kaishu.md";
  slug: "morioka/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"moriyama_shiga/2026-05-12-taiyoko.md": {
	id: "moriyama_shiga/2026-05-12-taiyoko.md";
  slug: "moriyama_shiga/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"moriyama_shiga/2026-05-29-taishin-kaishu.md": {
	id: "moriyama_shiga/2026-05-29-taishin-kaishu.md";
  slug: "moriyama_shiga/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"moriyama_shiga/2026-06-05-shoene-kaishu.md": {
	id: "moriyama_shiga/2026-06-05-shoene-kaishu.md";
  slug: "moriyama_shiga/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"moroyama/2026-05-10-shoene.md": {
	id: "moroyama/2026-05-10-shoene.md";
  slug: "moroyama/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"moroyama/2026-05-10-taiyoko.md": {
	id: "moroyama/2026-05-10-taiyoko.md";
  slug: "moroyama/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"moroyama/2026-06-03-taishin-kaishu.md": {
	id: "moroyama/2026-06-03-taishin-kaishu.md";
  slug: "moroyama/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"muko/2026-05-11-taiyoko.md": {
	id: "muko/2026-05-11-taiyoko.md";
  slug: "muko/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"muko/2026-06-03-taishin-kaishu.md": {
	id: "muko/2026-06-03-taishin-kaishu.md";
  slug: "muko/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"muko/2026-06-05-shoene-kaishu.md": {
	id: "muko/2026-06-05-shoene-kaishu.md";
  slug: "muko/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"musashimurayama/2026-05-12-taiyoko.md": {
	id: "musashimurayama/2026-05-12-taiyoko.md";
  slug: "musashimurayama/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"musashimurayama/2026-05-29-taishin-kaishu.md": {
	id: "musashimurayama/2026-05-29-taishin-kaishu.md";
  slug: "musashimurayama/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"musashimurayama/2026-06-05-shoene-kaishu.md": {
	id: "musashimurayama/2026-06-05-shoene-kaishu.md";
  slug: "musashimurayama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"musashino/2026-05-06-energy-josei.md": {
	id: "musashino/2026-05-06-energy-josei.md";
  slug: "musashino/2026-05-06-energy-josei";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"musashino/2026-05-29-taishin-kaishu.md": {
	id: "musashino/2026-05-29-taishin-kaishu.md";
  slug: "musashino/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"musashino/2026-06-05-shoene-kaishu.md": {
	id: "musashino/2026-06-05-shoene-kaishu.md";
  slug: "musashino/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nachikatsura/2026-05-12-iju.md": {
	id: "nachikatsura/2026-05-12-iju.md";
  slug: "nachikatsura/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nachikatsura/2026-06-03-taishin-kaishu.md": {
	id: "nachikatsura/2026-06-03-taishin-kaishu.md";
  slug: "nachikatsura/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nachikatsura/2026-06-05-shoene-kaishu.md": {
	id: "nachikatsura/2026-06-05-shoene-kaishu.md";
  slug: "nachikatsura/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagahama/2026-05-12-iju.md": {
	id: "nagahama/2026-05-12-iju.md";
  slug: "nagahama/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagahama/2026-05-12-taiyoko.md": {
	id: "nagahama/2026-05-12-taiyoko.md";
  slug: "nagahama/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagahama/2026-05-29-taishin-kaishu.md": {
	id: "nagahama/2026-05-29-taishin-kaishu.md";
  slug: "nagahama/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagahama/2026-06-05-shoene-kaishu.md": {
	id: "nagahama/2026-06-05-shoene-kaishu.md";
  slug: "nagahama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagakute/2026-05-10-jutaku-shoene.md": {
	id: "nagakute/2026-05-10-jutaku-shoene.md";
  slug: "nagakute/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagakute/2026-05-10-sogyo-shien.md": {
	id: "nagakute/2026-05-10-sogyo-shien.md";
  slug: "nagakute/2026-05-10-sogyo-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagakute/2026-05-29-taishin-kaishu.md": {
	id: "nagakute/2026-05-29-taishin-kaishu.md";
  slug: "nagakute/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagano/2026-05-27-iju-akiya.md": {
	id: "nagano/2026-05-27-iju-akiya.md";
  slug: "nagano/2026-05-27-iju-akiya";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagano/2026-05-27-taishin-kaishu.md": {
	id: "nagano/2026-05-27-taishin-kaishu.md";
  slug: "nagano/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagano/2026-06-05-shoene-kaishu.md": {
	id: "nagano/2026-06-05-shoene-kaishu.md";
  slug: "nagano/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagano_pref/2026-05-17-jgrants-cdy1amah.md": {
	id: "nagano_pref/2026-05-17-jgrants-cdy1amah.md";
  slug: "nagano_pref/2026-05-17-jgrants-cdy1amah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagano_pref/2026-05-17-jgrants-cdy1fmah.md": {
	id: "nagano_pref/2026-05-17-jgrants-cdy1fmah.md";
  slug: "nagano_pref/2026-05-17-jgrants-cdy1fmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagano_pref/2026-05-17-jgrants-cdy1gmax.md": {
	id: "nagano_pref/2026-05-17-jgrants-cdy1gmax.md";
  slug: "nagano_pref/2026-05-17-jgrants-cdy1gmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagano_pref/2026-05-17-jgrants-cdy1kmah.md": {
	id: "nagano_pref/2026-05-17-jgrants-cdy1kmah.md";
  slug: "nagano_pref/2026-05-17-jgrants-cdy1kmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagano_pref/2026-05-17-jgrants-cdy1lmax.md": {
	id: "nagano_pref/2026-05-17-jgrants-cdy1lmax.md";
  slug: "nagano_pref/2026-05-17-jgrants-cdy1lmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagano_pref/2026-05-17-jgrants-cdy1pmah.md": {
	id: "nagano_pref/2026-05-17-jgrants-cdy1pmah.md";
  slug: "nagano_pref/2026-05-17-jgrants-cdy1pmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagano_pref/2026-05-17-jgrants-cdy1umah.md": {
	id: "nagano_pref/2026-05-17-jgrants-cdy1umah.md";
  slug: "nagano_pref/2026-05-17-jgrants-cdy1umah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagano_pref/2026-05-17-jgrants-cdy1vmax.md": {
	id: "nagano_pref/2026-05-17-jgrants-cdy1vmax.md";
  slug: "nagano_pref/2026-05-17-jgrants-cdy1vmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagano_pref/2026-05-17-jgrants-cdy24mah.md": {
	id: "nagano_pref/2026-05-17-jgrants-cdy24mah.md";
  slug: "nagano_pref/2026-05-17-jgrants-cdy24mah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagano_pref/2026-05-17-jgrants-cdyoomah.md": {
	id: "nagano_pref/2026-05-17-jgrants-cdyoomah.md";
  slug: "nagano_pref/2026-05-17-jgrants-cdyoomah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagano_pref/2026-06-03-shoene-kaishu.md": {
	id: "nagano_pref/2026-06-03-shoene-kaishu.md";
  slug: "nagano_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagano_pref/2026-06-03-taishin-kaishu.md": {
	id: "nagano_pref/2026-06-03-taishin-kaishu.md";
  slug: "nagano_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagaoka/2026-05-29-jutaku-reform.md": {
	id: "nagaoka/2026-05-29-jutaku-reform.md";
  slug: "nagaoka/2026-05-29-jutaku-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagaoka/2026-05-29-taishin-kaishu.md": {
	id: "nagaoka/2026-05-29-taishin-kaishu.md";
  slug: "nagaoka/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagaoka/2026-06-05-shoene-kaishu.md": {
	id: "nagaoka/2026-06-05-shoene-kaishu.md";
  slug: "nagaoka/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagaokakyo/2026-05-11-taiyoko.md": {
	id: "nagaokakyo/2026-05-11-taiyoko.md";
  slug: "nagaokakyo/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagaokakyo/2026-05-29-taishin-kaishu.md": {
	id: "nagaokakyo/2026-05-29-taishin-kaishu.md";
  slug: "nagaokakyo/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagaokakyo/2026-06-05-shoene-kaishu.md": {
	id: "nagaokakyo/2026-06-05-shoene-kaishu.md";
  slug: "nagaokakyo/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagasaki/2026-05-24-challenge-hojokin.md": {
	id: "nagasaki/2026-05-24-challenge-hojokin.md";
  slug: "nagasaki/2026-05-24-challenge-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagasaki/2026-05-24-ijuu-shienkin.md": {
	id: "nagasaki/2026-05-24-ijuu-shienkin.md";
  slug: "nagasaki/2026-05-24-ijuu-shienkin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagasaki/2026-05-24-sogyo-seicyo-hojokin.md": {
	id: "nagasaki/2026-05-24-sogyo-seicyo-hojokin.md";
  slug: "nagasaki/2026-05-24-sogyo-seicyo-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagasaki/2026-05-27-kosodate-jutaku.md": {
	id: "nagasaki/2026-05-27-kosodate-jutaku.md";
  slug: "nagasaki/2026-05-27-kosodate-jutaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagasaki/2026-05-27-taiyoko-chikudenchi.md": {
	id: "nagasaki/2026-05-27-taiyoko-chikudenchi.md";
  slug: "nagasaki/2026-05-27-taiyoko-chikudenchi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagasaki/2026-05-29-sumai-reform.md": {
	id: "nagasaki/2026-05-29-sumai-reform.md";
  slug: "nagasaki/2026-05-29-sumai-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagasaki/2026-05-29-taishin-kaishu.md": {
	id: "nagasaki/2026-05-29-taishin-kaishu.md";
  slug: "nagasaki/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagasaki/2026-06-05-kaitekijutaku-hojo.md": {
	id: "nagasaki/2026-06-05-kaitekijutaku-hojo.md";
  slug: "nagasaki/2026-06-05-kaitekijutaku-hojo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagasaki/2026-06-05-shoene-kaishu.md": {
	id: "nagasaki/2026-06-05-shoene-kaishu.md";
  slug: "nagasaki/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagasaki_pref/2026-06-05-shoene-kaishu.md": {
	id: "nagasaki_pref/2026-06-05-shoene-kaishu.md";
  slug: "nagasaki_pref/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagasaki_pref/2026-06-05-taishin-kaishu.md": {
	id: "nagasaki_pref/2026-06-05-taishin-kaishu.md";
  slug: "nagasaki_pref/2026-06-05-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagatoro/2026-05-10-shoene.md": {
	id: "nagatoro/2026-05-10-shoene.md";
  slug: "nagatoro/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagatoro/2026-05-10-taiyoko.md": {
	id: "nagatoro/2026-05-10-taiyoko.md";
  slug: "nagatoro/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagatoro/2026-06-03-taishin-kaishu.md": {
	id: "nagatoro/2026-06-03-taishin-kaishu.md";
  slug: "nagatoro/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagoya/2026-05-10-jutaku-shoene.md": {
	id: "nagoya/2026-05-10-jutaku-shoene.md";
  slug: "nagoya/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagoya/2026-05-10-sogyo-shien.md": {
	id: "nagoya/2026-05-10-sogyo-shien.md";
  slug: "nagoya/2026-05-10-sogyo-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagoya/2026-05-10-taishin-kaishu.md": {
	id: "nagoya/2026-05-10-taishin-kaishu.md";
  slug: "nagoya/2026-05-10-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagoya/2026-05-19-aerospace-setsubi.md": {
	id: "nagoya/2026-05-19-aerospace-setsubi.md";
  slug: "nagoya/2026-05-19-aerospace-setsubi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagoya/2026-05-19-asbestos-taisaku.md": {
	id: "nagoya/2026-05-19-asbestos-taisaku.md";
  slug: "nagoya/2026-05-19-asbestos-taisaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagoya/2026-05-19-chuko-jutaku-rishi.md": {
	id: "nagoya/2026-05-19-chuko-jutaku-rishi.md";
  slug: "nagoya/2026-05-19-chuko-jutaku-rishi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagoya/2026-05-19-datsucarbon-hojokin.md": {
	id: "nagoya/2026-05-19-datsucarbon-hojokin.md";
  slug: "nagoya/2026-05-19-datsucarbon-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagoya/2026-05-19-green-reform-loan.md": {
	id: "nagoya/2026-05-19-green-reform-loan.md";
  slug: "nagoya/2026-05-19-green-reform-loan";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagoya/2026-05-19-kigyou-saito-shi.md": {
	id: "nagoya/2026-05-19-kigyou-saito-shi.md";
  slug: "nagoya/2026-05-19-kigyou-saito-shi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagoya/2026-05-19-midori-hojokin.md": {
	id: "nagoya/2026-05-19-midori-hojokin.md";
  slug: "nagoya/2026-05-19-midori-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagoya/2026-05-19-shinshin-breaker.md": {
	id: "nagoya/2026-05-19-shinshin-breaker.md";
  slug: "nagoya/2026-05-19-shinshin-breaker";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagoya/2026-05-25-kosodate-jutaku.md": {
	id: "nagoya/2026-05-25-kosodate-jutaku.md";
  slug: "nagoya/2026-05-25-kosodate-jutaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nagoya/2026-05-25-taiyoko-chikudenchi.md": {
	id: "nagoya/2026-05-25-taiyoko-chikudenchi.md";
  slug: "nagoya/2026-05-25-taiyoko-chikudenchi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"naha/2026-05-23-hanro-kakudai.md": {
	id: "naha/2026-05-23-hanro-kakudai.md";
  slug: "naha/2026-05-23-hanro-kakudai";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"naha/2026-06-03-taishin-kaishu.md": {
	id: "naha/2026-06-03-taishin-kaishu.md";
  slug: "naha/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"naha/2026-06-05-shoene-2026.md": {
	id: "naha/2026-06-05-shoene-2026.md";
  slug: "naha/2026-06-05-shoene-2026";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"naha/2026-06-05-shoene-kaishu.md": {
	id: "naha/2026-06-05-shoene-kaishu.md";
  slug: "naha/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nakai/2026-05-08-taiyoko-hojo.md": {
	id: "nakai/2026-05-08-taiyoko-hojo.md";
  slug: "nakai/2026-05-08-taiyoko-hojo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nakai/2026-06-03-taishin-kaishu.md": {
	id: "nakai/2026-06-03-taishin-kaishu.md";
  slug: "nakai/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nakai/2026-06-05-shoene-kaishu.md": {
	id: "nakai/2026-06-05-shoene-kaishu.md";
  slug: "nakai/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nakano/2026-05-05-setsubi-hojokin.md": {
	id: "nakano/2026-05-05-setsubi-hojokin.md";
  slug: "nakano/2026-05-05-setsubi-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nakano/2026-05-29-taishin-kaishu.md": {
	id: "nakano/2026-05-29-taishin-kaishu.md";
  slug: "nakano/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nakano/2026-06-05-shoene-kaishu.md": {
	id: "nakano/2026-06-05-shoene-kaishu.md";
  slug: "nakano/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nakatsu/2026-05-29-taishin-kaishu.md": {
	id: "nakatsu/2026-05-29-taishin-kaishu.md";
  slug: "nakatsu/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nakatsu/2026-06-05-shoene-kaishu.md": {
	id: "nakatsu/2026-06-05-shoene-kaishu.md";
  slug: "nakatsu/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"namegawa/2026-05-10-shoene.md": {
	id: "namegawa/2026-05-10-shoene.md";
  slug: "namegawa/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"namegawa/2026-05-10-taiyoko.md": {
	id: "namegawa/2026-05-10-taiyoko.md";
  slug: "namegawa/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"namegawa/2026-06-03-taishin-kaishu.md": {
	id: "namegawa/2026-06-03-taishin-kaishu.md";
  slug: "namegawa/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nantan/2026-05-11-iju.md": {
	id: "nantan/2026-05-11-iju.md";
  slug: "nantan/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nantan/2026-05-11-taiyoko.md": {
	id: "nantan/2026-05-11-taiyoko.md";
  slug: "nantan/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nantan/2026-05-29-taishin-kaishu.md": {
	id: "nantan/2026-05-29-taishin-kaishu.md";
  slug: "nantan/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nantan/2026-06-05-shoene-kaishu.md": {
	id: "nantan/2026-06-05-shoene-kaishu.md";
  slug: "nantan/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nara/2026-05-12-kosodate.md": {
	id: "nara/2026-05-12-kosodate.md";
  slug: "nara/2026-05-12-kosodate";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nara/2026-05-12-taiyoko.md": {
	id: "nara/2026-05-12-taiyoko.md";
  slug: "nara/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nara/2026-05-29-taishin-kaishu.md": {
	id: "nara/2026-05-29-taishin-kaishu.md";
  slug: "nara/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nara/2026-06-05-saiene-hojo.md": {
	id: "nara/2026-06-05-saiene-hojo.md";
  slug: "nara/2026-06-05-saiene-hojo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nara/2026-06-05-shoene-kaishu.md": {
	id: "nara/2026-06-05-shoene-kaishu.md";
  slug: "nara/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nara_pref/2026-05-12-iju.md": {
	id: "nara_pref/2026-05-12-iju.md";
  slug: "nara_pref/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nara_pref/2026-05-12-taiyoko.md": {
	id: "nara_pref/2026-05-12-taiyoko.md";
  slug: "nara_pref/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nara_pref/2026-05-17-jgrants-cdxy3mah.md": {
	id: "nara_pref/2026-05-17-jgrants-cdxy3mah.md";
  slug: "nara_pref/2026-05-17-jgrants-cdxy3mah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nara_pref/2026-05-17-jgrants-cdxy8mah.md": {
	id: "nara_pref/2026-05-17-jgrants-cdxy8mah.md";
  slug: "nara_pref/2026-05-17-jgrants-cdxy8mah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nara_pref/2026-05-17-jgrants-cdyfemax.md": {
	id: "nara_pref/2026-05-17-jgrants-cdyfemax.md";
  slug: "nara_pref/2026-05-17-jgrants-cdyfemax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nara_pref/2026-06-03-shoene-kaishu.md": {
	id: "nara_pref/2026-06-03-shoene-kaishu.md";
  slug: "nara_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nara_pref/2026-06-03-taishin-kaishu.md": {
	id: "nara_pref/2026-06-03-taishin-kaishu.md";
  slug: "nara_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nasushiobara/2026-05-29-taishin-kaishu.md": {
	id: "nasushiobara/2026-05-29-taishin-kaishu.md";
  slug: "nasushiobara/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nasushiobara/2026-06-05-shoene-kaishu.md": {
	id: "nasushiobara/2026-06-05-shoene-kaishu.md";
  slug: "nasushiobara/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nerima/2026-05-05-carbon-neutral.md": {
	id: "nerima/2026-05-05-carbon-neutral.md";
  slug: "nerima/2026-05-05-carbon-neutral";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nerima/2026-05-29-taishin-kaishu.md": {
	id: "nerima/2026-05-29-taishin-kaishu.md";
  slug: "nerima/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nerima/2026-06-05-shoene-kaishu.md": {
	id: "nerima/2026-06-05-shoene-kaishu.md";
  slug: "nerima/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"neyagawa/2026-05-01-sogyo-shoten.md": {
	id: "neyagawa/2026-05-01-sogyo-shoten.md";
  slug: "neyagawa/2026-05-01-sogyo-shoten";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"neyagawa/2026-05-04-solar.md": {
	id: "neyagawa/2026-05-04-solar.md";
  slug: "neyagawa/2026-05-04-solar";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"neyagawa/2026-05-29-taishin-kaishu.md": {
	id: "neyagawa/2026-05-29-taishin-kaishu.md";
  slug: "neyagawa/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"niigata/2026-05-23-6ji-sangyo.md": {
	id: "niigata/2026-05-23-6ji-sangyo.md";
  slug: "niigata/2026-05-23-6ji-sangyo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"niigata/2026-05-23-brand-hojokin.md": {
	id: "niigata/2026-05-23-brand-hojokin.md";
  slug: "niigata/2026-05-23-brand-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"niigata/2026-05-23-jinzaiikusei.md": {
	id: "niigata/2026-05-23-jinzaiikusei.md";
  slug: "niigata/2026-05-23-jinzaiikusei";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"niigata/2026-05-23-shouryokuka-shoene.md": {
	id: "niigata/2026-05-23-shouryokuka-shoene.md";
  slug: "niigata/2026-05-23-shouryokuka-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"niigata/2026-05-24-saiene-reform.md": {
	id: "niigata/2026-05-24-saiene-reform.md";
  slug: "niigata/2026-05-24-saiene-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"niigata/2026-05-25-ijuu-shienkin.md": {
	id: "niigata/2026-05-25-ijuu-shienkin.md";
  slug: "niigata/2026-05-25-ijuu-shienkin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"niigata/2026-05-27-kosodate-jutaku.md": {
	id: "niigata/2026-05-27-kosodate-jutaku.md";
  slug: "niigata/2026-05-27-kosodate-jutaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"niigata/2026-05-27-taiyoko-chikudenchi.md": {
	id: "niigata/2026-05-27-taiyoko-chikudenchi.md";
  slug: "niigata/2026-05-27-taiyoko-chikudenchi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"niigata/2026-05-29-taishin-kaishu.md": {
	id: "niigata/2026-05-29-taishin-kaishu.md";
  slug: "niigata/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"niigata/2026-06-05-saiene-導入-hojo.md": {
	id: "niigata/2026-06-05-saiene-導入-hojo.md";
  slug: "niigata/2026-06-05-saiene-導入-hojo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"niigata_pref/2026-05-17-jgrants-cdovoma5.md": {
	id: "niigata_pref/2026-05-17-jgrants-cdovoma5.md";
  slug: "niigata_pref/2026-05-17-jgrants-cdovoma5";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"niigata_pref/2026-05-17-jgrants-cdxuemap.md": {
	id: "niigata_pref/2026-05-17-jgrants-cdxuemap.md";
  slug: "niigata_pref/2026-05-17-jgrants-cdxuemap";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"niigata_pref/2026-06-03-shoene-kaishu.md": {
	id: "niigata_pref/2026-06-03-shoene-kaishu.md";
  slug: "niigata_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"niigata_pref/2026-06-03-taishin-kaishu.md": {
	id: "niigata_pref/2026-06-03-taishin-kaishu.md";
  slug: "niigata_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"niiza/2026-05-10-taiyoko.md": {
	id: "niiza/2026-05-10-taiyoko.md";
  slug: "niiza/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"niiza/2026-05-10-zero-carbon.md": {
	id: "niiza/2026-05-10-zero-carbon.md";
  slug: "niiza/2026-05-10-zero-carbon";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"niiza/2026-05-10-zero_carbon.md": {
	id: "niiza/2026-05-10-zero_carbon.md";
  slug: "niiza/2026-05-10-zero_carbon";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"niiza/2026-06-03-taishin-kaishu.md": {
	id: "niiza/2026-06-03-taishin-kaishu.md";
  slug: "niiza/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"niiza/2026-06-05-shoene-kaishu.md": {
	id: "niiza/2026-06-05-shoene-kaishu.md";
  slug: "niiza/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nikko/2026-06-05-shoene-kaishu.md": {
	id: "nikko/2026-06-05-shoene-kaishu.md";
  slug: "nikko/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nikko/2026-06-05-taishin-kaishu.md": {
	id: "nikko/2026-06-05-taishin-kaishu.md";
  slug: "nikko/2026-06-05-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ninomiya/2026-05-08-shoene-denka.md": {
	id: "ninomiya/2026-05-08-shoene-denka.md";
  slug: "ninomiya/2026-05-08-shoene-denka";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ninomiya/2026-06-03-taishin-kaishu.md": {
	id: "ninomiya/2026-06-03-taishin-kaishu.md";
  slug: "ninomiya/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nishinomiya/2026-05-11-taiyoko.md": {
	id: "nishinomiya/2026-05-11-taiyoko.md";
  slug: "nishinomiya/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nishinomiya/2026-05-11-zeh-jyutaku.md": {
	id: "nishinomiya/2026-05-11-zeh-jyutaku.md";
  slug: "nishinomiya/2026-05-11-zeh-jyutaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nishinomiya/2026-05-29-taishin-kaishu.md": {
	id: "nishinomiya/2026-05-29-taishin-kaishu.md";
  slug: "nishinomiya/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nishio/2026-05-10-jutaku-reform.md": {
	id: "nishio/2026-05-10-jutaku-reform.md";
  slug: "nishio/2026-05-10-jutaku-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nishio/2026-05-10-jutaku-shoene.md": {
	id: "nishio/2026-05-10-jutaku-shoene.md";
  slug: "nishio/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nishio/2026-05-10-sogyo-shien.md": {
	id: "nishio/2026-05-10-sogyo-shien.md";
  slug: "nishio/2026-05-10-sogyo-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nishio/2026-05-29-taishin-kaishu.md": {
	id: "nishio/2026-05-29-taishin-kaishu.md";
  slug: "nishio/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nishitokyo/2026-05-06-aircon-josei.md": {
	id: "nishitokyo/2026-05-06-aircon-josei.md";
  slug: "nishitokyo/2026-05-06-aircon-josei";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nishitokyo/2026-06-03-taishin-kaishu.md": {
	id: "nishitokyo/2026-06-03-taishin-kaishu.md";
  slug: "nishitokyo/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nishitokyo/2026-06-05-shoene-kaishu.md": {
	id: "nishitokyo/2026-06-05-shoene-kaishu.md";
  slug: "nishitokyo/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nishiwaki/2026-05-11-sogyo.md": {
	id: "nishiwaki/2026-05-11-sogyo.md";
  slug: "nishiwaki/2026-05-11-sogyo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nishiwaki/2026-05-11-taiyoko.md": {
	id: "nishiwaki/2026-05-11-taiyoko.md";
  slug: "nishiwaki/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nishiwaki/2026-05-29-taishin-kaishu.md": {
	id: "nishiwaki/2026-05-29-taishin-kaishu.md";
  slug: "nishiwaki/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nishiwaki/2026-06-05-shoene-kaishu.md": {
	id: "nishiwaki/2026-06-05-shoene-kaishu.md";
  slug: "nishiwaki/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nisshin/2026-05-10-jutaku-shoene.md": {
	id: "nisshin/2026-05-10-jutaku-shoene.md";
  slug: "nisshin/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nisshin/2026-05-10-sogyo-shien.md": {
	id: "nisshin/2026-05-10-sogyo-shien.md";
  slug: "nisshin/2026-05-10-sogyo-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nisshin/2026-06-03-taishin-kaishu.md": {
	id: "nisshin/2026-06-03-taishin-kaishu.md";
  slug: "nisshin/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nobeoka/2026-05-29-taishin-kaishu.md": {
	id: "nobeoka/2026-05-29-taishin-kaishu.md";
  slug: "nobeoka/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nobeoka/2026-06-05-shoene-kaishu.md": {
	id: "nobeoka/2026-06-05-shoene-kaishu.md";
  slug: "nobeoka/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nose/2026-05-04-jyugai.md": {
	id: "nose/2026-05-04-jyugai.md";
  slug: "nose/2026-05-04-jyugai";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nose/2026-05-04-shinsyu-noka.md": {
	id: "nose/2026-05-04-shinsyu-noka.md";
  slug: "nose/2026-05-04-shinsyu-noka";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nose/2026-06-03-taishin-kaishu.md": {
	id: "nose/2026-06-03-taishin-kaishu.md";
  slug: "nose/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"nose/2026-06-05-shoene-kaishu.md": {
	id: "nose/2026-06-05-shoene-kaishu.md";
  slug: "nose/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"numazu/2026-05-29-shoene-taiyoko.md": {
	id: "numazu/2026-05-29-shoene-taiyoko.md";
  slug: "numazu/2026-05-29-shoene-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"numazu/2026-05-29-taishin-kaishu.md": {
	id: "numazu/2026-05-29-taishin-kaishu.md";
  slug: "numazu/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"obu/2026-05-10-jutaku-shoene.md": {
	id: "obu/2026-05-10-jutaku-shoene.md";
  slug: "obu/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"obu/2026-05-10-sogyo-shien.md": {
	id: "obu/2026-05-10-sogyo-shien.md";
  slug: "obu/2026-05-10-sogyo-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"obu/2026-05-29-taishin-kaishu.md": {
	id: "obu/2026-05-29-taishin-kaishu.md";
  slug: "obu/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"odawara/2026-05-08-juuten-taiyoko.md": {
	id: "odawara/2026-05-08-juuten-taiyoko.md";
  slug: "odawara/2026-05-08-juuten-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"odawara/2026-05-12-iju.md": {
	id: "odawara/2026-05-12-iju.md";
  slug: "odawara/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"odawara/2026-05-29-taishin-kaishu.md": {
	id: "odawara/2026-05-29-taishin-kaishu.md";
  slug: "odawara/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"odawara/2026-06-05-shoene-kaishu.md": {
	id: "odawara/2026-06-05-shoene-kaishu.md";
  slug: "odawara/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ogaki/2026-05-29-taishin-kaishu.md": {
	id: "ogaki/2026-05-29-taishin-kaishu.md";
  slug: "ogaki/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ogaki/2026-06-05-shoene-kaishu.md": {
	id: "ogaki/2026-06-05-shoene-kaishu.md";
  slug: "ogaki/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ogano/2026-05-10-shoene.md": {
	id: "ogano/2026-05-10-shoene.md";
  slug: "ogano/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ogano/2026-05-10-taiyoko.md": {
	id: "ogano/2026-05-10-taiyoko.md";
  slug: "ogano/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ogano/2026-06-03-taishin-kaishu.md": {
	id: "ogano/2026-06-03-taishin-kaishu.md";
  slug: "ogano/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ogawa_s/2026-05-10-shoene.md": {
	id: "ogawa_s/2026-05-10-shoene.md";
  slug: "ogawa_s/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ogawa_s/2026-05-10-taiyoko.md": {
	id: "ogawa_s/2026-05-10-taiyoko.md";
  slug: "ogawa_s/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ogawa_s/2026-06-03-taishin-kaishu.md": {
	id: "ogawa_s/2026-06-03-taishin-kaishu.md";
  slug: "ogawa_s/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ogose/2026-05-10-shoene.md": {
	id: "ogose/2026-05-10-shoene.md";
  slug: "ogose/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ogose/2026-05-10-taiyoko.md": {
	id: "ogose/2026-05-10-taiyoko.md";
  slug: "ogose/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ogose/2026-06-03-taishin-kaishu.md": {
	id: "ogose/2026-06-03-taishin-kaishu.md";
  slug: "ogose/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oguchi/2026-05-10-shoene-aichi-pref.md": {
	id: "oguchi/2026-05-10-shoene-aichi-pref.md";
  slug: "oguchi/2026-05-10-shoene-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oguchi/2026-05-10-sogyo-aichi-pref.md": {
	id: "oguchi/2026-05-10-sogyo-aichi-pref.md";
  slug: "oguchi/2026-05-10-sogyo-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oguchi/2026-06-03-taishin-kaishu.md": {
	id: "oguchi/2026-06-03-taishin-kaishu.md";
  slug: "oguchi/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oharu/2026-05-10-shoene-aichi-pref.md": {
	id: "oharu/2026-05-10-shoene-aichi-pref.md";
  slug: "oharu/2026-05-10-shoene-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oharu/2026-05-10-sogyo-aichi-pref.md": {
	id: "oharu/2026-05-10-sogyo-aichi-pref.md";
  slug: "oharu/2026-05-10-sogyo-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oharu/2026-06-03-taishin-kaishu.md": {
	id: "oharu/2026-06-03-taishin-kaishu.md";
  slug: "oharu/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oi/2026-05-08-smart-energy.md": {
	id: "oi/2026-05-08-smart-energy.md";
  slug: "oi/2026-05-08-smart-energy";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oi/2026-06-03-taishin-kaishu.md": {
	id: "oi/2026-06-03-taishin-kaishu.md";
  slug: "oi/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oi/2026-06-05-shoene-kaishu.md": {
	id: "oi/2026-06-05-shoene-kaishu.md";
  slug: "oi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oiso/2026-05-08-smart-energy.md": {
	id: "oiso/2026-05-08-smart-energy.md";
  slug: "oiso/2026-05-08-smart-energy";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oiso/2026-06-03-taishin-kaishu.md": {
	id: "oiso/2026-06-03-taishin-kaishu.md";
  slug: "oiso/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oiso/2026-06-05-shoene-kaishu.md": {
	id: "oiso/2026-06-05-shoene-kaishu.md";
  slug: "oiso/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oita/2026-05-23-sogyo-hojokin.md": {
	id: "oita/2026-05-23-sogyo-hojokin.md";
  slug: "oita/2026-05-23-sogyo-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oita/2026-05-29-taishin-kaishu.md": {
	id: "oita/2026-05-29-taishin-kaishu.md";
  slug: "oita/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oita/2026-06-05-saiene-hojokin.md": {
	id: "oita/2026-06-05-saiene-hojokin.md";
  slug: "oita/2026-06-05-saiene-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oita/2026-06-05-shoene-kaishu.md": {
	id: "oita/2026-06-05-shoene-kaishu.md";
  slug: "oita/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oita_pref/2026-05-27-iju-shienkin.md": {
	id: "oita_pref/2026-05-27-iju-shienkin.md";
  slug: "oita_pref/2026-05-27-iju-shienkin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oita_pref/2026-05-27-taishin-kaishu.md": {
	id: "oita_pref/2026-05-27-taishin-kaishu.md";
  slug: "oita_pref/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oita_pref/2026-06-05-shoene-kaishu.md": {
	id: "oita_pref/2026-06-05-shoene-kaishu.md";
  slug: "oita_pref/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okayama/2026-05-27-taishin-kaishu.md": {
	id: "okayama/2026-05-27-taishin-kaishu.md";
  slug: "okayama/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okayama/2026-05-27-taiyoko.md": {
	id: "okayama/2026-05-27-taiyoko.md";
  slug: "okayama/2026-05-27-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okayama/2026-06-05-shoene-kaishu.md": {
	id: "okayama/2026-06-05-shoene-kaishu.md";
  slug: "okayama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okayama_pref/2026-05-17-jgrants-cdycxmap.md": {
	id: "okayama_pref/2026-05-17-jgrants-cdycxmap.md";
  slug: "okayama_pref/2026-05-17-jgrants-cdycxmap";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okayama_pref/2026-05-17-jgrants-cdyd0map.md": {
	id: "okayama_pref/2026-05-17-jgrants-cdyd0map.md";
  slug: "okayama_pref/2026-05-17-jgrants-cdyd0map";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okayama_pref/2026-05-17-jgrants-cdyu0map.md": {
	id: "okayama_pref/2026-05-17-jgrants-cdyu0map.md";
  slug: "okayama_pref/2026-05-17-jgrants-cdyu0map";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okayama_pref/2026-05-20-jgrants-cdyysmax.md": {
	id: "okayama_pref/2026-05-20-jgrants-cdyysmax.md";
  slug: "okayama_pref/2026-05-20-jgrants-cdyysmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okayama_pref/2026-06-03-shoene-kaishu.md": {
	id: "okayama_pref/2026-06-03-shoene-kaishu.md";
  slug: "okayama_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okayama_pref/2026-06-03-taishin-kaishu.md": {
	id: "okayama_pref/2026-06-03-taishin-kaishu.md";
  slug: "okayama_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okazaki/2026-05-10-ijyu-teijyu.md": {
	id: "okazaki/2026-05-10-ijyu-teijyu.md";
  slug: "okazaki/2026-05-10-ijyu-teijyu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okazaki/2026-05-10-jutaku-shoene.md": {
	id: "okazaki/2026-05-10-jutaku-shoene.md";
  slug: "okazaki/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okazaki/2026-05-10-sogyo-shien.md": {
	id: "okazaki/2026-05-10-sogyo-shien.md";
  slug: "okazaki/2026-05-10-sogyo-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okazaki/2026-05-29-taishin-kaishu.md": {
	id: "okazaki/2026-05-29-taishin-kaishu.md";
  slug: "okazaki/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okazaki/2026-06-05-ondankataisaku-shoene.md": {
	id: "okazaki/2026-06-05-ondankataisaku-shoene.md";
  slug: "okazaki/2026-06-05-ondankataisaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okegawa/2026-05-10-datsutanso.md": {
	id: "okegawa/2026-05-10-datsutanso.md";
  slug: "okegawa/2026-05-10-datsutanso";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okegawa/2026-05-10-pref-saiene.md": {
	id: "okegawa/2026-05-10-pref-saiene.md";
  slug: "okegawa/2026-05-10-pref-saiene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okegawa/2026-05-10-pref.md": {
	id: "okegawa/2026-05-10-pref.md";
  slug: "okegawa/2026-05-10-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okegawa/2026-06-03-taishin-kaishu.md": {
	id: "okegawa/2026-06-03-taishin-kaishu.md";
  slug: "okegawa/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okegawa/2026-06-05-shoene-kaishu.md": {
	id: "okegawa/2026-06-05-shoene-kaishu.md";
  slug: "okegawa/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okinawa_pref/2026-05-27-taishin-taifu.md": {
	id: "okinawa_pref/2026-05-27-taishin-taifu.md";
  slug: "okinawa_pref/2026-05-27-taishin-taifu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okinawa_pref/2026-06-05-shoene-kaishu.md": {
	id: "okinawa_pref/2026-06-05-shoene-kaishu.md";
  slug: "okinawa_pref/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okutama/2026-05-12-iju.md": {
	id: "okutama/2026-05-12-iju.md";
  slug: "okutama/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okutama/2026-05-12-taiyoko.md": {
	id: "okutama/2026-05-12-taiyoko.md";
  slug: "okutama/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okutama/2026-06-03-taishin-kaishu.md": {
	id: "okutama/2026-06-03-taishin-kaishu.md";
  slug: "okutama/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"okutama/2026-06-05-shoene-kaishu.md": {
	id: "okutama/2026-06-05-shoene-kaishu.md";
  slug: "okutama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ome/2026-05-12-taiyoko.md": {
	id: "ome/2026-05-12-taiyoko.md";
  slug: "ome/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ome/2026-06-03-taishin-kaishu.md": {
	id: "ome/2026-06-03-taishin-kaishu.md";
  slug: "ome/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ome/2026-06-05-shoene-kaishu.md": {
	id: "ome/2026-06-05-shoene-kaishu.md";
  slug: "ome/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"omihachiman/2026-05-12-taiyoko.md": {
	id: "omihachiman/2026-05-12-taiyoko.md";
  slug: "omihachiman/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"omihachiman/2026-05-29-taishin-kaishu.md": {
	id: "omihachiman/2026-05-29-taishin-kaishu.md";
  slug: "omihachiman/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"omihachiman/2026-06-05-shoene-kaishu.md": {
	id: "omihachiman/2026-06-05-shoene-kaishu.md";
  slug: "omihachiman/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ono_hyogo/2026-05-11-taiyoko.md": {
	id: "ono_hyogo/2026-05-11-taiyoko.md";
  slug: "ono_hyogo/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ono_hyogo/2026-05-11-teijyu.md": {
	id: "ono_hyogo/2026-05-11-teijyu.md";
  slug: "ono_hyogo/2026-05-11-teijyu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ono_hyogo/2026-05-29-taishin-kaishu.md": {
	id: "ono_hyogo/2026-05-29-taishin-kaishu.md";
  slug: "ono_hyogo/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ono_hyogo/2026-06-05-shoene-kaishu.md": {
	id: "ono_hyogo/2026-06-05-shoene-kaishu.md";
  slug: "ono_hyogo/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka/2026-04-26-fukushi-page-0000622098.md": {
	id: "osaka/2026-04-26-fukushi-page-0000622098.md";
  slug: "osaka/2026-04-26-fukushi-page-0000622098";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka/2026-04-26-fukushi-page-0000639409.md": {
	id: "osaka/2026-04-26-fukushi-page-0000639409.md";
  slug: "osaka/2026-04-26-fukushi-page-0000639409";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka/2026-04-26-kodomo-page-0000349320.md": {
	id: "osaka/2026-04-26-kodomo-page-0000349320.md";
  slug: "osaka/2026-04-26-kodomo-page-0000349320";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka/2026-04-26-kodomo-page-0000369443.md": {
	id: "osaka/2026-04-26-kodomo-page-0000369443.md";
  slug: "osaka/2026-04-26-kodomo-page-0000369443";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka/2026-04-26-kodomo-page-0000370608.md": {
	id: "osaka/2026-04-26-kodomo-page-0000370608.md";
  slug: "osaka/2026-04-26-kodomo-page-0000370608";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka/2026-04-26-kodomo-page-0000452094.md": {
	id: "osaka/2026-04-26-kodomo-page-0000452094.md";
  slug: "osaka/2026-04-26-kodomo-page-0000452094";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka/2026-04-26-kodomo-page-0000667973.md": {
	id: "osaka/2026-04-26-kodomo-page-0000667973.md";
  slug: "osaka/2026-04-26-kodomo-page-0000667973";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka/2026-04-26-kodomo-page-0000677764.md": {
	id: "osaka/2026-04-26-kodomo-page-0000677764.md";
  slug: "osaka/2026-04-26-kodomo-page-0000677764";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka/2026-04-27-toshiseibi-page-0000669809.md": {
	id: "osaka/2026-04-27-toshiseibi-page-0000669809.md";
  slug: "osaka/2026-04-27-toshiseibi-page-0000669809";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka/2026-04-28-keizaisenryaku-page-0000674956.md": {
	id: "osaka/2026-04-28-keizaisenryaku-page-0000674956.md";
  slug: "osaka/2026-04-28-keizaisenryaku-page-0000674956";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka/2026-04-28-keizaisenryaku-page-0000678258.md": {
	id: "osaka/2026-04-28-keizaisenryaku-page-0000678258.md";
  slug: "osaka/2026-04-28-keizaisenryaku-page-0000678258";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka/2026-05-24-shoene-kaishu.md": {
	id: "osaka/2026-05-24-shoene-kaishu.md";
  slug: "osaka/2026-05-24-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka/2026-05-25-chikudenchi-hojokin.md": {
	id: "osaka/2026-05-25-chikudenchi-hojokin.md";
  slug: "osaka/2026-05-25-chikudenchi-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka/2026-05-29-taishin-kaishu.md": {
	id: "osaka/2026-05-29-taishin-kaishu.md";
  slug: "osaka/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_izumi/2026-05-03-chusho-shien.md": {
	id: "osaka_izumi/2026-05-03-chusho-shien.md";
  slug: "osaka_izumi/2026-05-03-chusho-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_izumi/2026-05-03-sogyo-shien.md": {
	id: "osaka_izumi/2026-05-03-sogyo-shien.md";
  slug: "osaka_izumi/2026-05-03-sogyo-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_izumi/2026-05-20-jgrants-cdyvpmax.md": {
	id: "osaka_izumi/2026-05-20-jgrants-cdyvpmax.md";
  slug: "osaka_izumi/2026-05-20-jgrants-cdyvpmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_izumi/2026-05-29-taishin-kaishu.md": {
	id: "osaka_izumi/2026-05-29-taishin-kaishu.md";
  slug: "osaka_izumi/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_izumi/2026-06-05-shoene-kaishu.md": {
	id: "osaka_izumi/2026-06-05-shoene-kaishu.md";
  slug: "osaka_izumi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_pref/2026-04-29-jugyoryo-genmen.md": {
	id: "osaka_pref/2026-04-29-jugyoryo-genmen.md";
  slug: "osaka_pref/2026-04-29-jugyoryo-genmen";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_pref/2026-04-29-riekiritur8.md": {
	id: "osaka_pref/2026-04-29-riekiritur8.md";
  slug: "osaka_pref/2026-04-29-riekiritur8";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_pref/2026-04-29-shigaku-mushoka-r8.md": {
	id: "osaka_pref/2026-04-29-shigaku-mushoka-r8.md";
  slug: "osaka_pref/2026-04-29-shigaku-mushoka-r8";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_pref/2026-04-29-shougaku-kyuuhu-kyuhen.md": {
	id: "osaka_pref/2026-04-29-shougaku-kyuuhu-kyuhen.md";
  slug: "osaka_pref/2026-04-29-shougaku-kyuuhu-kyuhen";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_pref/2026-04-29-shougaku-kyuuhu.md": {
	id: "osaka_pref/2026-04-29-shougaku-kyuuhu.md";
  slug: "osaka_pref/2026-04-29-shougaku-kyuuhu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_pref/2026-04-29-syuttenshien-r8.md": {
	id: "osaka_pref/2026-04-29-syuttenshien-r8.md";
  slug: "osaka_pref/2026-04-29-syuttenshien-r8";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_pref/2026-05-04-fukugyo.md": {
	id: "osaka_pref/2026-05-04-fukugyo.md";
  slug: "osaka_pref/2026-05-04-fukugyo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_pref/2026-05-04-gyomu-kaizen.md": {
	id: "osaka_pref/2026-05-04-gyomu-kaizen.md";
  slug: "osaka_pref/2026-05-04-gyomu-kaizen";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_pref/2026-05-04-shinsyu-noka.md": {
	id: "osaka_pref/2026-05-04-shinsyu-noka.md";
  slug: "osaka_pref/2026-05-04-shinsyu-noka";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_pref/2026-05-04-solar-kyodo.md": {
	id: "osaka_pref/2026-05-04-solar-kyodo.md";
  slug: "osaka_pref/2026-05-04-solar-kyodo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_pref/2026-05-17-jgrants-cdxfhmah.md": {
	id: "osaka_pref/2026-05-17-jgrants-cdxfhmah.md";
  slug: "osaka_pref/2026-05-17-jgrants-cdxfhmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_pref/2026-05-17-jgrants-cdxfmmah.md": {
	id: "osaka_pref/2026-05-17-jgrants-cdxfmmah.md";
  slug: "osaka_pref/2026-05-17-jgrants-cdxfmmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_pref/2026-05-17-jgrants-cdxqimap.md": {
	id: "osaka_pref/2026-05-17-jgrants-cdxqimap.md";
  slug: "osaka_pref/2026-05-17-jgrants-cdxqimap";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_pref/2026-05-17-jgrants-cdyvqmax.md": {
	id: "osaka_pref/2026-05-17-jgrants-cdyvqmax.md";
  slug: "osaka_pref/2026-05-17-jgrants-cdyvqmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_pref/2026-05-17-jgrants-cdyvrmax.md": {
	id: "osaka_pref/2026-05-17-jgrants-cdyvrmax.md";
  slug: "osaka_pref/2026-05-17-jgrants-cdyvrmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_pref/2026-05-20-jgrants-cdyo4mah.md": {
	id: "osaka_pref/2026-05-20-jgrants-cdyo4mah.md";
  slug: "osaka_pref/2026-05-20-jgrants-cdyo4mah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_pref/2026-06-03-shoene-kaishu.md": {
	id: "osaka_pref/2026-06-03-shoene-kaishu.md";
  slug: "osaka_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_pref/2026-06-03-taishin-kaishu.md": {
	id: "osaka_pref/2026-06-03-taishin-kaishu.md";
  slug: "osaka_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_sayama/2026-05-04-sogyo.md": {
	id: "osaka_sayama/2026-05-04-sogyo.md";
  slug: "osaka_sayama/2026-05-04-sogyo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_sayama/2026-05-04-taishin.md": {
	id: "osaka_sayama/2026-05-04-taishin.md";
  slug: "osaka_sayama/2026-05-04-taishin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaka_sayama/2026-06-05-shoene-kaishu.md": {
	id: "osaka_sayama/2026-06-05-shoene-kaishu.md";
  slug: "osaka_sayama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaki/2026-05-29-taishin-iju.md": {
	id: "osaki/2026-05-29-taishin-iju.md";
  slug: "osaki/2026-05-29-taishin-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"osaki/2026-06-05-shoene-kaishu.md": {
	id: "osaki/2026-06-05-shoene-kaishu.md";
  slug: "osaki/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ota_gunma/2026-05-29-taishin-kaishu.md": {
	id: "ota_gunma/2026-05-29-taishin-kaishu.md";
  slug: "ota_gunma/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ota_gunma/2026-06-05-shoene-kaishu.md": {
	id: "ota_gunma/2026-06-05-shoene-kaishu.md";
  slug: "ota_gunma/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ota_ku/2026-05-05-reform-josei.md": {
	id: "ota_ku/2026-05-05-reform-josei.md";
  slug: "ota_ku/2026-05-05-reform-josei";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ota_ku/2026-05-29-taishin-kaishu.md": {
	id: "ota_ku/2026-05-29-taishin-kaishu.md";
  slug: "ota_ku/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ota_ku/2026-06-05-shoene-kaishu.md": {
	id: "ota_ku/2026-06-05-shoene-kaishu.md";
  slug: "ota_ku/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"otsu/2026-05-12-kosodate.md": {
	id: "otsu/2026-05-12-kosodate.md";
  slug: "otsu/2026-05-12-kosodate";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"otsu/2026-05-12-taiyoko.md": {
	id: "otsu/2026-05-12-taiyoko.md";
  slug: "otsu/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"otsu/2026-06-03-taishin-kaishu.md": {
	id: "otsu/2026-06-03-taishin-kaishu.md";
  slug: "otsu/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"otsu/2026-06-05-shoene-kaishu.md": {
	id: "otsu/2026-06-05-shoene-kaishu.md";
  slug: "otsu/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"otsu/2026-06-05-smart-lifestyle-hojo.md": {
	id: "otsu/2026-06-05-smart-lifestyle-hojo.md";
  slug: "otsu/2026-06-05-smart-lifestyle-hojo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"owariasahi/2026-05-10-jutaku-reform.md": {
	id: "owariasahi/2026-05-10-jutaku-reform.md";
  slug: "owariasahi/2026-05-10-jutaku-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"owariasahi/2026-05-10-jutaku-shoene.md": {
	id: "owariasahi/2026-05-10-jutaku-shoene.md";
  slug: "owariasahi/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"owariasahi/2026-06-03-taishin-kaishu.md": {
	id: "owariasahi/2026-06-03-taishin-kaishu.md";
  slug: "owariasahi/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oyama/2026-05-29-taishin-kaishu.md": {
	id: "oyama/2026-05-29-taishin-kaishu.md";
  slug: "oyama/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oyama/2026-06-05-shoene-kaishu.md": {
	id: "oyama/2026-06-05-shoene-kaishu.md";
  slug: "oyama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oyamazaki/2026-05-11-taiyoko.md": {
	id: "oyamazaki/2026-05-11-taiyoko.md";
  slug: "oyamazaki/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oyamazaki/2026-05-29-taishin-kaishu.md": {
	id: "oyamazaki/2026-05-29-taishin-kaishu.md";
  slug: "oyamazaki/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"oyamazaki/2026-06-05-shoene-kaishu.md": {
	id: "oyamazaki/2026-06-05-shoene-kaishu.md";
  slug: "oyamazaki/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ranzan/2026-05-10-shoene.md": {
	id: "ranzan/2026-05-10-shoene.md";
  slug: "ranzan/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ranzan/2026-05-10-taiyoko.md": {
	id: "ranzan/2026-05-10-taiyoko.md";
  slug: "ranzan/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ranzan/2026-06-03-taishin-kaishu.md": {
	id: "ranzan/2026-06-03-taishin-kaishu.md";
  slug: "ranzan/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ritto/2026-05-12-taiyoko.md": {
	id: "ritto/2026-05-12-taiyoko.md";
  slug: "ritto/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ritto/2026-05-29-taishin-kaishu.md": {
	id: "ritto/2026-05-29-taishin-kaishu.md";
  slug: "ritto/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ritto/2026-06-05-shoene-kaishu.md": {
	id: "ritto/2026-06-05-shoene-kaishu.md";
  slug: "ritto/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ryuou/2026-05-12-taiyoko.md": {
	id: "ryuou/2026-05-12-taiyoko.md";
  slug: "ryuou/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ryuou/2026-05-29-taishin-kaishu.md": {
	id: "ryuou/2026-05-29-taishin-kaishu.md";
  slug: "ryuou/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ryuou/2026-06-05-shoene-kaishu.md": {
	id: "ryuou/2026-06-05-shoene-kaishu.md";
  slug: "ryuou/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saga/2026-05-27-taishin-kaishu.md": {
	id: "saga/2026-05-27-taishin-kaishu.md";
  slug: "saga/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saga/2026-06-05-shoene-dannetsu.md": {
	id: "saga/2026-06-05-shoene-dannetsu.md";
  slug: "saga/2026-06-05-shoene-dannetsu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saga/2026-06-05-shoene-kaishu.md": {
	id: "saga/2026-06-05-shoene-kaishu.md";
  slug: "saga/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saga_pref/2026-05-17-jgrants-cdy7zmax.md": {
	id: "saga_pref/2026-05-17-jgrants-cdy7zmax.md";
  slug: "saga_pref/2026-05-17-jgrants-cdy7zmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saga_pref/2026-06-03-shoene-kaishu.md": {
	id: "saga_pref/2026-06-03-shoene-kaishu.md";
  slug: "saga_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saga_pref/2026-06-03-taishin-kaishu.md": {
	id: "saga_pref/2026-06-03-taishin-kaishu.md";
  slug: "saga_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sagamihara/2026-05-08-smart-energy.md": {
	id: "sagamihara/2026-05-08-smart-energy.md";
  slug: "sagamihara/2026-05-08-smart-energy";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sagamihara/2026-05-12-chusho.md": {
	id: "sagamihara/2026-05-12-chusho.md";
  slug: "sagamihara/2026-05-12-chusho";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sagamihara/2026-05-29-taishin-kaishu.md": {
	id: "sagamihara/2026-05-29-taishin-kaishu.md";
  slug: "sagamihara/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sagamihara/2026-06-05-shoene-kaishu.md": {
	id: "sagamihara/2026-06-05-shoene-kaishu.md";
  slug: "sagamihara/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sagamihara/2026-06-05-shoene-smart-energy.md": {
	id: "sagamihara/2026-06-05-shoene-smart-energy.md";
  slug: "sagamihara/2026-06-05-shoene-smart-energy";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saitama/2026-05-10-shoene-jutaku.md": {
	id: "saitama/2026-05-10-shoene-jutaku.md";
  slug: "saitama/2026-05-10-shoene-jutaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saitama/2026-05-10-taiyoko-kyodokounyuu.md": {
	id: "saitama/2026-05-10-taiyoko-kyodokounyuu.md";
  slug: "saitama/2026-05-10-taiyoko-kyodokounyuu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saitama/2026-05-10-taiyoko.md": {
	id: "saitama/2026-05-10-taiyoko.md";
  slug: "saitama/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saitama/2026-05-20-nougyou-shinkou-hojokin.md": {
	id: "saitama/2026-05-20-nougyou-shinkou-hojokin.md";
  slug: "saitama/2026-05-20-nougyou-shinkou-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saitama/2026-05-20-shoene-dannetsu-hojokin.md": {
	id: "saitama/2026-05-20-shoene-dannetsu-hojokin.md";
  slug: "saitama/2026-05-20-shoene-dannetsu-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saitama/2026-05-24-shoene-dannetsu.md": {
	id: "saitama/2026-05-24-shoene-dannetsu.md";
  slug: "saitama/2026-05-24-shoene-dannetsu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saitama/2026-05-29-taishin-kaishu.md": {
	id: "saitama/2026-05-29-taishin-kaishu.md";
  slug: "saitama/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saitama/2026-06-05-shoene-dannetsu-hojo.md": {
	id: "saitama/2026-06-05-shoene-dannetsu-hojo.md";
  slug: "saitama/2026-06-05-shoene-dannetsu-hojo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saitama_pref/2026-05-10-co2-chusho.md": {
	id: "saitama_pref/2026-05-10-co2-chusho.md";
  slug: "saitama_pref/2026-05-10-co2-chusho";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saitama_pref/2026-05-10-kigyo-shien.md": {
	id: "saitama_pref/2026-05-10-kigyo-shien.md";
  slug: "saitama_pref/2026-05-10-kigyo-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saitama_pref/2026-05-10-shoene-saiene.md": {
	id: "saitama_pref/2026-05-10-shoene-saiene.md";
  slug: "saitama_pref/2026-05-10-shoene-saiene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saitama_pref/2026-05-17-jgrants-cdxqmmax.md": {
	id: "saitama_pref/2026-05-17-jgrants-cdxqmmax.md";
  slug: "saitama_pref/2026-05-17-jgrants-cdxqmmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saitama_pref/2026-05-17-jgrants-cdy6gmax.md": {
	id: "saitama_pref/2026-05-17-jgrants-cdy6gmax.md";
  slug: "saitama_pref/2026-05-17-jgrants-cdy6gmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saitama_pref/2026-05-17-jgrants-cdyoemax.md": {
	id: "saitama_pref/2026-05-17-jgrants-cdyoemax.md";
  slug: "saitama_pref/2026-05-17-jgrants-cdyoemax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saitama_pref/2026-05-17-jgrants-cdyuxma5.md": {
	id: "saitama_pref/2026-05-17-jgrants-cdyuxma5.md";
  slug: "saitama_pref/2026-05-17-jgrants-cdyuxma5";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saitama_pref/2026-06-03-shoene-kaishu.md": {
	id: "saitama_pref/2026-06-03-shoene-kaishu.md";
  slug: "saitama_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"saitama_pref/2026-06-03-taishin-kaishu.md": {
	id: "saitama_pref/2026-06-03-taishin-kaishu.md";
  slug: "saitama_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sakado/2026-05-10-kaden.md": {
	id: "sakado/2026-05-10-kaden.md";
  slug: "sakado/2026-05-10-kaden";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sakado/2026-05-10-taiyoko.md": {
	id: "sakado/2026-05-10-taiyoko.md";
  slug: "sakado/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sakado/2026-06-03-taishin-kaishu.md": {
	id: "sakado/2026-06-03-taishin-kaishu.md";
  slug: "sakado/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sakado/2026-06-05-shoene-kaishu.md": {
	id: "sakado/2026-06-05-shoene-kaishu.md";
  slug: "sakado/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sakai/2026-04-29-akiya.md": {
	id: "sakai/2026-04-29-akiya.md";
  slug: "sakai/2026-04-29-akiya";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sakai/2026-04-29-digitalka.md": {
	id: "sakai/2026-04-29-digitalka.md";
  slug: "sakai/2026-04-29-digitalka";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sakai/2026-04-29-izumigaoka-rent.md": {
	id: "sakai/2026-04-29-izumigaoka-rent.md";
  slug: "sakai/2026-04-29-izumigaoka-rent";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sakai/2026-04-29-monochalle.md": {
	id: "sakai/2026-04-29-monochalle.md";
  slug: "sakai/2026-04-29-monochalle";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sakai/2026-04-29-nakamozu-office.md": {
	id: "sakai/2026-04-29-nakamozu-office.md";
  slug: "sakai/2026-04-29-nakamozu-office";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sakai/2026-04-29-nakamozu-rent.md": {
	id: "sakai/2026-04-29-nakamozu-rent.md";
  slug: "sakai/2026-04-29-nakamozu-rent";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sakai/2026-04-29-reskilling.md": {
	id: "sakai/2026-04-29-reskilling.md";
  slug: "sakai/2026-04-29-reskilling";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sakai/2026-04-29-saiene.md": {
	id: "sakai/2026-04-29-saiene.md";
  slug: "sakai/2026-04-29-saiene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sakai/2026-04-29-toshin-rent.md": {
	id: "sakai/2026-04-29-toshin-rent.md";
  slug: "sakai/2026-04-29-toshin-rent";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sakai/2026-05-29-taishin-kaishu.md": {
	id: "sakai/2026-05-29-taishin-kaishu.md";
  slug: "sakai/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sakai/2026-06-05-shoene-kaishu.md": {
	id: "sakai/2026-06-05-shoene-kaishu.md";
  slug: "sakai/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sakata/2026-05-29-taishin-kaishu.md": {
	id: "sakata/2026-05-29-taishin-kaishu.md";
  slug: "sakata/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sakata/2026-06-05-shoene-kaishu.md": {
	id: "sakata/2026-06-05-shoene-kaishu.md";
  slug: "sakata/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sakurai_nara/2026-05-12-taiyoko.md": {
	id: "sakurai_nara/2026-05-12-taiyoko.md";
  slug: "sakurai_nara/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sakurai_nara/2026-05-29-taishin-kaishu.md": {
	id: "sakurai_nara/2026-05-29-taishin-kaishu.md";
  slug: "sakurai_nara/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sakurai_nara/2026-06-05-shoene-kaishu.md": {
	id: "sakurai_nara/2026-06-05-shoene-kaishu.md";
  slug: "sakurai_nara/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"samukawa/2026-05-08-zero-carbon-josei.md": {
	id: "samukawa/2026-05-08-zero-carbon-josei.md";
  slug: "samukawa/2026-05-08-zero-carbon-josei";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"samukawa/2026-06-03-taishin-kaishu.md": {
	id: "samukawa/2026-06-03-taishin-kaishu.md";
  slug: "samukawa/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"samukawa/2026-06-05-shoene-kaishu.md": {
	id: "samukawa/2026-06-05-shoene-kaishu.md";
  slug: "samukawa/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sanda/2026-05-11-iju.md": {
	id: "sanda/2026-05-11-iju.md";
  slug: "sanda/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sanda/2026-05-11-taiyoko.md": {
	id: "sanda/2026-05-11-taiyoko.md";
  slug: "sanda/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sanda/2026-05-29-taishin-kaishu.md": {
	id: "sanda/2026-05-29-taishin-kaishu.md";
  slug: "sanda/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sanda/2026-06-05-shoene-kaishu.md": {
	id: "sanda/2026-06-05-shoene-kaishu.md";
  slug: "sanda/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sapporo/2026-05-20-eco-reform.md": {
	id: "sapporo/2026-05-20-eco-reform.md";
  slug: "sapporo/2026-05-20-eco-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sapporo/2026-05-20-mokuzou-taishin.md": {
	id: "sapporo/2026-05-20-mokuzou-taishin.md";
  slug: "sapporo/2026-05-20-mokuzou-taishin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sapporo/2026-05-20-saiene-shoene-hojokin.md": {
	id: "sapporo/2026-05-20-saiene-shoene-hojokin.md";
  slug: "sapporo/2026-05-20-saiene-shoene-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"satte/2026-05-10-shoene.md": {
	id: "satte/2026-05-10-shoene.md";
  slug: "satte/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"satte/2026-05-10-taiyoko.md": {
	id: "satte/2026-05-10-taiyoko.md";
  slug: "satte/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"satte/2026-06-03-taishin-kaishu.md": {
	id: "satte/2026-06-03-taishin-kaishu.md";
  slug: "satte/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sayama/2026-05-10-kaden.md": {
	id: "sayama/2026-05-10-kaden.md";
  slug: "sayama/2026-05-10-kaden";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sayama/2026-05-10-saiene.md": {
	id: "sayama/2026-05-10-saiene.md";
  slug: "sayama/2026-05-10-saiene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sayama/2026-05-10-sub2.md": {
	id: "sayama/2026-05-10-sub2.md";
  slug: "sayama/2026-05-10-sub2";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sayama/2026-06-03-taishin-kaishu.md": {
	id: "sayama/2026-06-03-taishin-kaishu.md";
  slug: "sayama/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sayama/2026-06-05-shoene-kaishu.md": {
	id: "sayama/2026-06-05-shoene-kaishu.md";
  slug: "sayama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sayo/2026-05-11-iju.md": {
	id: "sayo/2026-05-11-iju.md";
  slug: "sayo/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sayo/2026-05-11-taiyoko.md": {
	id: "sayo/2026-05-11-taiyoko.md";
  slug: "sayo/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sayo/2026-05-29-taishin-kaishu.md": {
	id: "sayo/2026-05-29-taishin-kaishu.md";
  slug: "sayo/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sayo/2026-06-05-shoene-kaishu.md": {
	id: "sayo/2026-06-05-shoene-kaishu.md";
  slug: "sayo/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"seika/2026-05-11-taiyoko.md": {
	id: "seika/2026-05-11-taiyoko.md";
  slug: "seika/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"seika/2026-05-29-taishin-kaishu.md": {
	id: "seika/2026-05-29-taishin-kaishu.md";
  slug: "seika/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"seika/2026-06-05-shoene-kaishu.md": {
	id: "seika/2026-06-05-shoene-kaishu.md";
  slug: "seika/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sendai/2026-05-23-shinseihinkaihatsu.md": {
	id: "sendai/2026-05-23-shinseihinkaihatsu.md";
  slug: "sendai/2026-05-23-shinseihinkaihatsu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sendai/2026-05-24-taiyoko-shoene.md": {
	id: "sendai/2026-05-24-taiyoko-shoene.md";
  slug: "sendai/2026-05-24-taiyoko-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sendai/2026-05-25-ijuu-shienkin.md": {
	id: "sendai/2026-05-25-ijuu-shienkin.md";
  slug: "sendai/2026-05-25-ijuu-shienkin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sendai/2026-05-25-kosodate-jutaku.md": {
	id: "sendai/2026-05-25-kosodate-jutaku.md";
  slug: "sendai/2026-05-25-kosodate-jutaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sendai/2026-05-25-taishin-kaishu.md": {
	id: "sendai/2026-05-25-taishin-kaishu.md";
  slug: "sendai/2026-05-25-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sendai/2026-06-05-shoene-kaishu.md": {
	id: "sendai/2026-06-05-shoene-kaishu.md";
  slug: "sendai/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sennan/2026-05-03-akitenpo.md": {
	id: "sennan/2026-05-03-akitenpo.md";
  slug: "sennan/2026-05-03-akitenpo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sennan/2026-05-03-kigyo-yuuchi.md": {
	id: "sennan/2026-05-03-kigyo-yuuchi.md";
  slug: "sennan/2026-05-03-kigyo-yuuchi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sennan/2026-06-03-taishin-kaishu.md": {
	id: "sennan/2026-06-03-taishin-kaishu.md";
  slug: "sennan/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sennan/2026-06-05-shoene-kaishu.md": {
	id: "sennan/2026-06-05-shoene-kaishu.md";
  slug: "sennan/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"setagaya/2026-05-05-eco-jutaku.md": {
	id: "setagaya/2026-05-05-eco-jutaku.md";
  slug: "setagaya/2026-05-05-eco-jutaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"setagaya/2026-05-05-zutto-setagaya.md": {
	id: "setagaya/2026-05-05-zutto-setagaya.md";
  slug: "setagaya/2026-05-05-zutto-setagaya";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"setagaya/2026-05-29-taishin-kaishu.md": {
	id: "setagaya/2026-05-29-taishin-kaishu.md";
  slug: "setagaya/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"setagaya/2026-06-05-shoene-kaishu.md": {
	id: "setagaya/2026-06-05-shoene-kaishu.md";
  slug: "setagaya/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"seto/2026-05-10-jutaku-reform.md": {
	id: "seto/2026-05-10-jutaku-reform.md";
  slug: "seto/2026-05-10-jutaku-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"seto/2026-05-10-jutaku-shoene.md": {
	id: "seto/2026-05-10-jutaku-shoene.md";
  slug: "seto/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"seto/2026-06-03-taishin-kaishu.md": {
	id: "seto/2026-06-03-taishin-kaishu.md";
  slug: "seto/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"settsu/2026-05-04-chusho-ikusei.md": {
	id: "settsu/2026-05-04-chusho-ikusei.md";
  slug: "settsu/2026-05-04-chusho-ikusei";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"settsu/2026-05-04-solar.md": {
	id: "settsu/2026-05-04-solar.md";
  slug: "settsu/2026-05-04-solar";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"settsu/2026-05-29-taishin-kaishu.md": {
	id: "settsu/2026-05-29-taishin-kaishu.md";
  slug: "settsu/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shibuya/2026-05-05-tenpo-kaigyou.md": {
	id: "shibuya/2026-05-05-tenpo-kaigyou.md";
  slug: "shibuya/2026-05-05-tenpo-kaigyou";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shibuya/2026-05-29-taishin-kaishu.md": {
	id: "shibuya/2026-05-29-taishin-kaishu.md";
  slug: "shibuya/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shibuya/2026-06-05-shoene-kaishu.md": {
	id: "shibuya/2026-06-05-shoene-kaishu.md";
  slug: "shibuya/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shiga_pref/2026-05-12-chusho.md": {
	id: "shiga_pref/2026-05-12-chusho.md";
  slug: "shiga_pref/2026-05-12-chusho";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shiga_pref/2026-05-12-taiyoko.md": {
	id: "shiga_pref/2026-05-12-taiyoko.md";
  slug: "shiga_pref/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shiga_pref/2026-05-17-jgrants-cdylnmah.md": {
	id: "shiga_pref/2026-05-17-jgrants-cdylnmah.md";
  slug: "shiga_pref/2026-05-17-jgrants-cdylnmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shiga_pref/2026-06-03-shoene-kaishu.md": {
	id: "shiga_pref/2026-06-03-shoene-kaishu.md";
  slug: "shiga_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shiga_pref/2026-06-03-taishin-kaishu.md": {
	id: "shiga_pref/2026-06-03-taishin-kaishu.md";
  slug: "shiga_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shijonawate/2026-05-01-akiya-reform.md": {
	id: "shijonawate/2026-05-01-akiya-reform.md";
  slug: "shijonawate/2026-05-01-akiya-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shijonawate/2026-05-01-kuchouki.md": {
	id: "shijonawate/2026-05-01-kuchouki.md";
  slug: "shijonawate/2026-05-01-kuchouki";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shijonawate/2026-06-03-taishin-kaishu.md": {
	id: "shijonawate/2026-06-03-taishin-kaishu.md";
  slug: "shijonawate/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shijonawate/2026-06-05-shoene-kaishu.md": {
	id: "shijonawate/2026-06-05-shoene-kaishu.md";
  slug: "shijonawate/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shiki/2026-05-10-shoene.md": {
	id: "shiki/2026-05-10-shoene.md";
  slug: "shiki/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shiki/2026-05-10-taiyoko.md": {
	id: "shiki/2026-05-10-taiyoko.md";
  slug: "shiki/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shiki/2026-06-03-taishin-kaishu.md": {
	id: "shiki/2026-06-03-taishin-kaishu.md";
  slug: "shiki/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shimane_pref/2026-05-17-jgrants-cdyrhmah.md": {
	id: "shimane_pref/2026-05-17-jgrants-cdyrhmah.md";
  slug: "shimane_pref/2026-05-17-jgrants-cdyrhmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shimane_pref/2026-06-03-shoene-kaishu.md": {
	id: "shimane_pref/2026-06-03-shoene-kaishu.md";
  slug: "shimane_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shimane_pref/2026-06-03-taishin-kaishu.md": {
	id: "shimane_pref/2026-06-03-taishin-kaishu.md";
  slug: "shimane_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shimonoseki/2026-05-29-taishin-kaishu.md": {
	id: "shimonoseki/2026-05-29-taishin-kaishu.md";
  slug: "shimonoseki/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shimonoseki/2026-06-05-shoene-kaishu.md": {
	id: "shimonoseki/2026-06-05-shoene-kaishu.md";
  slug: "shimonoseki/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shinagawa/2026-05-05-zero-carbon.md": {
	id: "shinagawa/2026-05-05-zero-carbon.md";
  slug: "shinagawa/2026-05-05-zero-carbon";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shinagawa/2026-05-29-taishin-kaishu.md": {
	id: "shinagawa/2026-05-29-taishin-kaishu.md";
  slug: "shinagawa/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shinagawa/2026-06-05-shoene-kaishu.md": {
	id: "shinagawa/2026-06-05-shoene-kaishu.md";
  slug: "shinagawa/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shingu/2026-05-12-iju.md": {
	id: "shingu/2026-05-12-iju.md";
  slug: "shingu/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shingu/2026-05-12-taiyoko.md": {
	id: "shingu/2026-05-12-taiyoko.md";
  slug: "shingu/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shingu/2026-06-03-taishin-kaishu.md": {
	id: "shingu/2026-06-03-taishin-kaishu.md";
  slug: "shingu/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shingu/2026-06-05-shoene-kaishu.md": {
	id: "shingu/2026-06-05-shoene-kaishu.md";
  slug: "shingu/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shinjuku/2026-05-05-taishin.md": {
	id: "shinjuku/2026-05-05-taishin.md";
  slug: "shinjuku/2026-05-05-taishin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shinjuku/2026-06-05-shoene-kaishu.md": {
	id: "shinjuku/2026-06-05-shoene-kaishu.md";
  slug: "shinjuku/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shinonsen/2026-05-11-iju.md": {
	id: "shinonsen/2026-05-11-iju.md";
  slug: "shinonsen/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shinonsen/2026-05-11-taiyoko.md": {
	id: "shinonsen/2026-05-11-taiyoko.md";
  slug: "shinonsen/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shinonsen/2026-06-03-taishin-kaishu.md": {
	id: "shinonsen/2026-06-03-taishin-kaishu.md";
  slug: "shinonsen/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shinonsen/2026-06-05-shoene-kaishu.md": {
	id: "shinonsen/2026-06-05-shoene-kaishu.md";
  slug: "shinonsen/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shinshiro/2026-05-10-jutaku-reform.md": {
	id: "shinshiro/2026-05-10-jutaku-reform.md";
  slug: "shinshiro/2026-05-10-jutaku-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shinshiro/2026-05-10-jutaku-shoene.md": {
	id: "shinshiro/2026-05-10-jutaku-shoene.md";
  slug: "shinshiro/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shinshiro/2026-06-03-taishin-kaishu.md": {
	id: "shinshiro/2026-06-03-taishin-kaishu.md";
  slug: "shinshiro/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shirahama/2026-05-12-iju.md": {
	id: "shirahama/2026-05-12-iju.md";
  slug: "shirahama/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shirahama/2026-05-12-taiyoko.md": {
	id: "shirahama/2026-05-12-taiyoko.md";
  slug: "shirahama/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shirahama/2026-06-03-taishin-kaishu.md": {
	id: "shirahama/2026-06-03-taishin-kaishu.md";
  slug: "shirahama/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shirahama/2026-06-05-shoene-kaishu.md": {
	id: "shirahama/2026-06-05-shoene-kaishu.md";
  slug: "shirahama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shiraoka/2026-05-10-shoene.md": {
	id: "shiraoka/2026-05-10-shoene.md";
  slug: "shiraoka/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shiraoka/2026-05-10-taiyoko.md": {
	id: "shiraoka/2026-05-10-taiyoko.md";
  slug: "shiraoka/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shiraoka/2026-06-03-taishin-kaishu.md": {
	id: "shiraoka/2026-06-03-taishin-kaishu.md";
  slug: "shiraoka/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shiso/2026-05-11-iju.md": {
	id: "shiso/2026-05-11-iju.md";
  slug: "shiso/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shiso/2026-05-11-taiyoko.md": {
	id: "shiso/2026-05-11-taiyoko.md";
  slug: "shiso/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shiso/2026-06-03-taishin-kaishu.md": {
	id: "shiso/2026-06-03-taishin-kaishu.md";
  slug: "shiso/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shiso/2026-06-05-shoene-kaishu.md": {
	id: "shiso/2026-06-05-shoene-kaishu.md";
  slug: "shiso/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shitara/2026-05-10-shoene-aichi-pref.md": {
	id: "shitara/2026-05-10-shoene-aichi-pref.md";
  slug: "shitara/2026-05-10-shoene-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shitara/2026-05-10-sogyo-aichi-pref.md": {
	id: "shitara/2026-05-10-sogyo-aichi-pref.md";
  slug: "shitara/2026-05-10-sogyo-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shitara/2026-06-03-taishin-kaishu.md": {
	id: "shitara/2026-06-03-taishin-kaishu.md";
  slug: "shitara/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shizuoka/2026-05-23-digital-hojokin.md": {
	id: "shizuoka/2026-05-23-digital-hojokin.md";
  slug: "shizuoka/2026-05-23-digital-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shizuoka/2026-05-23-shinsehin-hanbaro.md": {
	id: "shizuoka/2026-05-23-shinsehin-hanbaro.md";
  slug: "shizuoka/2026-05-23-shinsehin-hanbaro";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shizuoka/2026-05-23-startup-ritchi.md": {
	id: "shizuoka/2026-05-23-startup-ritchi.md";
  slug: "shizuoka/2026-05-23-startup-ritchi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shizuoka/2026-05-27-akiya-kaishu.md": {
	id: "shizuoka/2026-05-27-akiya-kaishu.md";
  slug: "shizuoka/2026-05-27-akiya-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shizuoka/2026-05-27-taishin-kaishu.md": {
	id: "shizuoka/2026-05-27-taishin-kaishu.md";
  slug: "shizuoka/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shizuoka/2026-06-05-shoene-kaishu.md": {
	id: "shizuoka/2026-06-05-shoene-kaishu.md";
  slug: "shizuoka/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shizuoka_pref/2026-05-17-jgrants-cdycfmax.md": {
	id: "shizuoka_pref/2026-05-17-jgrants-cdycfmax.md";
  slug: "shizuoka_pref/2026-05-17-jgrants-cdycfmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shizuoka_pref/2026-05-20-jgrants-cdysrmax.md": {
	id: "shizuoka_pref/2026-05-20-jgrants-cdysrmax.md";
  slug: "shizuoka_pref/2026-05-20-jgrants-cdysrmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shizuoka_pref/2026-06-03-shoene-kaishu.md": {
	id: "shizuoka_pref/2026-06-03-shoene-kaishu.md";
  slug: "shizuoka_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"shizuoka_pref/2026-06-03-taishin-kaishu.md": {
	id: "shizuoka_pref/2026-06-03-taishin-kaishu.md";
  slug: "shizuoka_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"soka/2026-05-10-shoene.md": {
	id: "soka/2026-05-10-shoene.md";
  slug: "soka/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"soka/2026-05-10-taiyoko.md": {
	id: "soka/2026-05-10-taiyoko.md";
  slug: "soka/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"soka/2026-05-29-taishin-kaishu.md": {
	id: "soka/2026-05-29-taishin-kaishu.md";
  slug: "soka/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"suginami/2026-05-05-eco-jutaku.md": {
	id: "suginami/2026-05-05-eco-jutaku.md";
  slug: "suginami/2026-05-05-eco-jutaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"suginami/2026-05-29-taishin-kaishu.md": {
	id: "suginami/2026-05-29-taishin-kaishu.md";
  slug: "suginami/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"suginami/2026-06-05-shoene-kaishu.md": {
	id: "suginami/2026-06-05-shoene-kaishu.md";
  slug: "suginami/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sugito/2026-05-10-shoene.md": {
	id: "sugito/2026-05-10-shoene.md";
  slug: "sugito/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sugito/2026-05-10-taiyoko.md": {
	id: "sugito/2026-05-10-taiyoko.md";
  slug: "sugito/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sugito/2026-06-03-taishin-kaishu.md": {
	id: "sugito/2026-06-03-taishin-kaishu.md";
  slug: "sugito/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"suita/2026-04-29-kosodate-1018219-1018222-1005675.md": {
	id: "suita/2026-04-29-kosodate-1018219-1018222-1005675.md";
  slug: "suita/2026-04-29-kosodate-1018219-1018222-1005675";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"suita/2026-04-29-kurashi-1018501-1018468-1018478-1010111.md": {
	id: "suita/2026-04-29-kurashi-1018501-1018468-1018478-1010111.md";
  slug: "suita/2026-04-29-kurashi-1018501-1018468-1018478-1010111";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"suita/2026-04-29-sangyo-1018028-1018029-1018030-1011571.md": {
	id: "suita/2026-04-29-sangyo-1018028-1018029-1018030-1011571.md";
  slug: "suita/2026-04-29-sangyo-1018028-1018029-1018030-1011571";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"suita/2026-04-29-sangyo-1018028-1018029-1018030-1011732.md": {
	id: "suita/2026-04-29-sangyo-1018028-1018029-1018030-1011732.md";
  slug: "suita/2026-04-29-sangyo-1018028-1018029-1018030-1011732";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"suita/2026-04-29-sangyo-1018028-1018029-1018030-1034178.md": {
	id: "suita/2026-04-29-sangyo-1018028-1018029-1018030-1034178.md";
  slug: "suita/2026-04-29-sangyo-1018028-1018029-1018030-1034178";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"suita/2026-04-29-sangyo-1018028-1018029-1018030-1038716.md": {
	id: "suita/2026-04-29-sangyo-1018028-1018029-1018030-1038716.md";
  slug: "suita/2026-04-29-sangyo-1018028-1018029-1018030-1038716";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"suita/2026-04-29-sangyo-1018028-1018029-1018030-1041621.md": {
	id: "suita/2026-04-29-sangyo-1018028-1018029-1018030-1041621.md";
  slug: "suita/2026-04-29-sangyo-1018028-1018029-1018030-1041621";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"suita/2026-04-29-sangyo-1018028-1018029-1018030-1043691.md": {
	id: "suita/2026-04-29-sangyo-1018028-1018029-1018030-1043691.md";
  slug: "suita/2026-04-29-sangyo-1018028-1018029-1018030-1043691";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"suita/2026-04-29-sangyo-1018028-1018029-1021242-1011520.md": {
	id: "suita/2026-04-29-sangyo-1018028-1018029-1021242-1011520.md";
  slug: "suita/2026-04-29-sangyo-1018028-1018029-1021242-1011520";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"suita/2026-05-29-taishin-kaishu.md": {
	id: "suita/2026-05-29-taishin-kaishu.md";
  slug: "suita/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"suita/2026-06-05-shoene-kaishu.md": {
	id: "suita/2026-06-05-shoene-kaishu.md";
  slug: "suita/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sumida/2026-05-05-ondanka-boshi.md": {
	id: "sumida/2026-05-05-ondanka-boshi.md";
  slug: "sumida/2026-05-05-ondanka-boshi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sumida/2026-05-29-taishin-kaishu.md": {
	id: "sumida/2026-05-29-taishin-kaishu.md";
  slug: "sumida/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sumida/2026-06-05-shoene-kaishu.md": {
	id: "sumida/2026-06-05-shoene-kaishu.md";
  slug: "sumida/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sumoto/2026-05-11-iju.md": {
	id: "sumoto/2026-05-11-iju.md";
  slug: "sumoto/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sumoto/2026-05-11-taiyoko.md": {
	id: "sumoto/2026-05-11-taiyoko.md";
  slug: "sumoto/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sumoto/2026-05-29-taishin-kaishu.md": {
	id: "sumoto/2026-05-29-taishin-kaishu.md";
  slug: "sumoto/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"sumoto/2026-06-05-shoene-kaishu.md": {
	id: "sumoto/2026-06-05-shoene-kaishu.md";
  slug: "sumoto/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"suzuka/2026-05-29-taishin-kaishu.md": {
	id: "suzuka/2026-05-29-taishin-kaishu.md";
  slug: "suzuka/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"suzuka/2026-06-05-shoene-kaishu.md": {
	id: "suzuka/2026-06-05-shoene-kaishu.md";
  slug: "suzuka/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tachikawa/2026-05-06-shoene-kaishu.md": {
	id: "tachikawa/2026-05-06-shoene-kaishu.md";
  slug: "tachikawa/2026-05-06-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tachikawa/2026-05-29-taishin-kaishu.md": {
	id: "tachikawa/2026-05-29-taishin-kaishu.md";
  slug: "tachikawa/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tadaoka/2026-05-04-levelup.md": {
	id: "tadaoka/2026-05-04-levelup.md";
  slug: "tadaoka/2026-05-04-levelup";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tadaoka/2026-05-04-shoene.md": {
	id: "tadaoka/2026-05-04-shoene.md";
  slug: "tadaoka/2026-05-04-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tadaoka/2026-06-03-taishin-kaishu.md": {
	id: "tadaoka/2026-06-03-taishin-kaishu.md";
  slug: "tadaoka/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"taga/2026-05-12-taiyoko.md": {
	id: "taga/2026-05-12-taiyoko.md";
  slug: "taga/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"taga/2026-05-29-taishin-kaishu.md": {
	id: "taga/2026-05-29-taishin-kaishu.md";
  slug: "taga/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"taga/2026-06-05-shoene-kaishu.md": {
	id: "taga/2026-06-05-shoene-kaishu.md";
  slug: "taga/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tahara/2026-05-10-jutaku-reform.md": {
	id: "tahara/2026-05-10-jutaku-reform.md";
  slug: "tahara/2026-05-10-jutaku-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tahara/2026-05-10-jutaku-shoene.md": {
	id: "tahara/2026-05-10-jutaku-shoene.md";
  slug: "tahara/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tahara/2026-06-03-taishin-kaishu.md": {
	id: "tahara/2026-06-03-taishin-kaishu.md";
  slug: "tahara/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"taishi/2026-05-04-sansei-dukyo.md": {
	id: "taishi/2026-05-04-sansei-dukyo.md";
  slug: "taishi/2026-05-04-sansei-dukyo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"taishi/2026-05-04-sogyo.md": {
	id: "taishi/2026-05-04-sogyo.md";
  slug: "taishi/2026-05-04-sogyo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"taishi/2026-06-03-taishin-kaishu.md": {
	id: "taishi/2026-06-03-taishin-kaishu.md";
  slug: "taishi/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"taishi/2026-06-05-shoene-kaishu.md": {
	id: "taishi/2026-06-05-shoene-kaishu.md";
  slug: "taishi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"taishi_h/2026-05-11-iju.md": {
	id: "taishi_h/2026-05-11-iju.md";
  slug: "taishi_h/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"taishi_h/2026-05-11-taiyoko.md": {
	id: "taishi_h/2026-05-11-taiyoko.md";
  slug: "taishi_h/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"taishi_h/2026-06-03-taishin-kaishu.md": {
	id: "taishi_h/2026-06-03-taishin-kaishu.md";
  slug: "taishi_h/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"taishi_h/2026-06-05-shoene-kaishu.md": {
	id: "taishi_h/2026-06-05-shoene-kaishu.md";
  slug: "taishi_h/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"taito/2026-05-05-datsu-tanso.md": {
	id: "taito/2026-05-05-datsu-tanso.md";
  slug: "taito/2026-05-05-datsu-tanso";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"taito/2026-05-29-taishin-kaishu.md": {
	id: "taito/2026-05-29-taishin-kaishu.md";
  slug: "taito/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"taito/2026-06-05-shoene-kaishu.md": {
	id: "taito/2026-06-05-shoene-kaishu.md";
  slug: "taito/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tajiri/2026-05-04-sansei-josei.md": {
	id: "tajiri/2026-05-04-sansei-josei.md";
  slug: "tajiri/2026-05-04-sansei-josei";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tajiri/2026-06-03-taishin-kaishu.md": {
	id: "tajiri/2026-06-03-taishin-kaishu.md";
  slug: "tajiri/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tajiri/2026-06-05-shoene-kaishu.md": {
	id: "tajiri/2026-06-05-shoene-kaishu.md";
  slug: "tajiri/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"taka_machi/2026-05-11-iju.md": {
	id: "taka_machi/2026-05-11-iju.md";
  slug: "taka_machi/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"taka_machi/2026-05-11-taiyoko.md": {
	id: "taka_machi/2026-05-11-taiyoko.md";
  slug: "taka_machi/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"taka_machi/2026-06-03-taishin-kaishu.md": {
	id: "taka_machi/2026-06-03-taishin-kaishu.md";
  slug: "taka_machi/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"taka_machi/2026-06-05-shoene-kaishu.md": {
	id: "taka_machi/2026-06-05-shoene-kaishu.md";
  slug: "taka_machi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takahama/2026-05-10-jutaku-reform.md": {
	id: "takahama/2026-05-10-jutaku-reform.md";
  slug: "takahama/2026-05-10-jutaku-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takahama/2026-05-10-jutaku-shoene.md": {
	id: "takahama/2026-05-10-jutaku-shoene.md";
  slug: "takahama/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takahama/2026-06-03-taishin-kaishu.md": {
	id: "takahama/2026-06-03-taishin-kaishu.md";
  slug: "takahama/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takaishi/2026-05-04-shikaku.md": {
	id: "takaishi/2026-05-04-shikaku.md";
  slug: "takaishi/2026-05-04-shikaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takaishi/2026-05-04-zero-carbon.md": {
	id: "takaishi/2026-05-04-zero-carbon.md";
  slug: "takaishi/2026-05-04-zero-carbon";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takaishi/2026-05-29-taishin-kaishu.md": {
	id: "takaishi/2026-05-29-taishin-kaishu.md";
  slug: "takaishi/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takaishi/2026-06-05-shoene-kaishu.md": {
	id: "takaishi/2026-06-05-shoene-kaishu.md";
  slug: "takaishi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takamatsu/2026-05-27-taishin-kaishu.md": {
	id: "takamatsu/2026-05-27-taishin-kaishu.md";
  slug: "takamatsu/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takamatsu/2026-06-05-shoene-kaishu.md": {
	id: "takamatsu/2026-06-05-shoene-kaishu.md";
  slug: "takamatsu/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takamatsu/2026-06-05-smart-house-hojo.md": {
	id: "takamatsu/2026-06-05-smart-house-hojo.md";
  slug: "takamatsu/2026-06-05-smart-house-hojo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takaoka/2026-05-29-taishin-kaishu.md": {
	id: "takaoka/2026-05-29-taishin-kaishu.md";
  slug: "takaoka/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takaoka/2026-06-05-shoene-kaishu.md": {
	id: "takaoka/2026-06-05-shoene-kaishu.md";
  slug: "takaoka/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takarazuka/2026-05-11-shoene.md": {
	id: "takarazuka/2026-05-11-shoene.md";
  slug: "takarazuka/2026-05-11-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takarazuka/2026-05-11-taiyoko.md": {
	id: "takarazuka/2026-05-11-taiyoko.md";
  slug: "takarazuka/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takarazuka/2026-05-29-taishin-kaishu.md": {
	id: "takarazuka/2026-05-29-taishin-kaishu.md";
  slug: "takarazuka/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takasago/2026-05-11-chusho.md": {
	id: "takasago/2026-05-11-chusho.md";
  slug: "takasago/2026-05-11-chusho";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takasago/2026-05-11-taiyoko.md": {
	id: "takasago/2026-05-11-taiyoko.md";
  slug: "takasago/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takasago/2026-05-29-taishin-kaishu.md": {
	id: "takasago/2026-05-29-taishin-kaishu.md";
  slug: "takasago/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takasago/2026-06-05-shoene-kaishu.md": {
	id: "takasago/2026-06-05-shoene-kaishu.md";
  slug: "takasago/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takasaki/2026-05-29-taishin-kaishu.md": {
	id: "takasaki/2026-05-29-taishin-kaishu.md";
  slug: "takasaki/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takasaki/2026-06-05-shoene-kaishu.md": {
	id: "takasaki/2026-06-05-shoene-kaishu.md";
  slug: "takasaki/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takashima/2026-05-12-iju.md": {
	id: "takashima/2026-05-12-iju.md";
  slug: "takashima/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takashima/2026-05-12-taiyoko.md": {
	id: "takashima/2026-05-12-taiyoko.md";
  slug: "takashima/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takashima/2026-05-29-taishin-kaishu.md": {
	id: "takashima/2026-05-29-taishin-kaishu.md";
  slug: "takashima/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takashima/2026-06-05-shoene-kaishu.md": {
	id: "takashima/2026-06-05-shoene-kaishu.md";
  slug: "takashima/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takatsuki/2026-04-30-kigyoteichaku.md": {
	id: "takatsuki/2026-04-30-kigyoteichaku.md";
  slug: "takatsuki/2026-04-30-kigyoteichaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takatsuki/2026-04-30-shaku.md": {
	id: "takatsuki/2026-04-30-shaku.md";
  slug: "takatsuki/2026-04-30-shaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takatsuki/2026-04-30-taishin.md": {
	id: "takatsuki/2026-04-30-taishin.md";
  slug: "takatsuki/2026-04-30-taishin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"takatsuki/2026-06-05-shoene-kaishu.md": {
	id: "takatsuki/2026-06-05-shoene-kaishu.md";
  slug: "takatsuki/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"taketoyo/2026-05-10-shoene-aichi-pref.md": {
	id: "taketoyo/2026-05-10-shoene-aichi-pref.md";
  slug: "taketoyo/2026-05-10-shoene-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"taketoyo/2026-05-10-sogyo-aichi-pref.md": {
	id: "taketoyo/2026-05-10-sogyo-aichi-pref.md";
  slug: "taketoyo/2026-05-10-sogyo-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"taketoyo/2026-06-03-taishin-kaishu.md": {
	id: "taketoyo/2026-06-03-taishin-kaishu.md";
  slug: "taketoyo/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tama/2026-05-12-taiyoko.md": {
	id: "tama/2026-05-12-taiyoko.md";
  slug: "tama/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tama/2026-05-20-jgrants-cdyvxmax.md": {
	id: "tama/2026-05-20-jgrants-cdyvxmax.md";
  slug: "tama/2026-05-20-jgrants-cdyvxmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tama/2026-06-03-taishin-kaishu.md": {
	id: "tama/2026-06-03-taishin-kaishu.md";
  slug: "tama/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tama/2026-06-05-shoene-kaishu.md": {
	id: "tama/2026-06-05-shoene-kaishu.md";
  slug: "tama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tamba/2026-05-11-iju.md": {
	id: "tamba/2026-05-11-iju.md";
  slug: "tamba/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tamba/2026-05-11-taiyoko.md": {
	id: "tamba/2026-05-11-taiyoko.md";
  slug: "tamba/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tamba/2026-06-03-taishin-kaishu.md": {
	id: "tamba/2026-06-03-taishin-kaishu.md";
  slug: "tamba/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tamba/2026-06-05-shoene-kaishu.md": {
	id: "tamba/2026-06-05-shoene-kaishu.md";
  slug: "tamba/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tanabe/2026-05-12-iju.md": {
	id: "tanabe/2026-05-12-iju.md";
  slug: "tanabe/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tanabe/2026-05-12-taiyoko.md": {
	id: "tanabe/2026-05-12-taiyoko.md";
  slug: "tanabe/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tanabe/2026-06-03-taishin-kaishu.md": {
	id: "tanabe/2026-06-03-taishin-kaishu.md";
  slug: "tanabe/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tanabe/2026-06-05-shoene-kaishu.md": {
	id: "tanabe/2026-06-05-shoene-kaishu.md";
  slug: "tanabe/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tanba/2026-05-11-iju.md": {
	id: "tanba/2026-05-11-iju.md";
  slug: "tanba/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tanba/2026-05-11-taiyoko.md": {
	id: "tanba/2026-05-11-taiyoko.md";
  slug: "tanba/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tanba/2026-05-29-taishin-kaishu.md": {
	id: "tanba/2026-05-29-taishin-kaishu.md";
  slug: "tanba/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tanba/2026-06-05-shoene-kaishu.md": {
	id: "tanba/2026-06-05-shoene-kaishu.md";
  slug: "tanba/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tanbasasayama/2026-05-11-iju.md": {
	id: "tanbasasayama/2026-05-11-iju.md";
  slug: "tanbasasayama/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tanbasasayama/2026-05-11-taiyoko.md": {
	id: "tanbasasayama/2026-05-11-taiyoko.md";
  slug: "tanbasasayama/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tanbasasayama/2026-05-29-taishin-kaishu.md": {
	id: "tanbasasayama/2026-05-29-taishin-kaishu.md";
  slug: "tanbasasayama/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tanbasasayama/2026-06-05-shoene-kaishu.md": {
	id: "tanbasasayama/2026-06-05-shoene-kaishu.md";
  slug: "tanbasasayama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tatsuno/2026-05-11-iju.md": {
	id: "tatsuno/2026-05-11-iju.md";
  slug: "tatsuno/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tatsuno/2026-05-11-taiyoko.md": {
	id: "tatsuno/2026-05-11-taiyoko.md";
  slug: "tatsuno/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tatsuno/2026-05-29-taishin-kaishu.md": {
	id: "tatsuno/2026-05-29-taishin-kaishu.md";
  slug: "tatsuno/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tatsuno/2026-06-05-shoene-kaishu.md": {
	id: "tatsuno/2026-06-05-shoene-kaishu.md";
  slug: "tatsuno/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tawaramoto/2026-05-12-taiyoko.md": {
	id: "tawaramoto/2026-05-12-taiyoko.md";
  slug: "tawaramoto/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tawaramoto/2026-06-03-taishin-kaishu.md": {
	id: "tawaramoto/2026-06-03-taishin-kaishu.md";
  slug: "tawaramoto/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tawaramoto/2026-06-05-shoene-kaishu.md": {
	id: "tawaramoto/2026-06-05-shoene-kaishu.md";
  slug: "tawaramoto/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tenkawa/2026-05-12-iju.md": {
	id: "tenkawa/2026-05-12-iju.md";
  slug: "tenkawa/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tenkawa/2026-06-03-taishin-kaishu.md": {
	id: "tenkawa/2026-06-03-taishin-kaishu.md";
  slug: "tenkawa/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tenkawa/2026-06-05-shoene-kaishu.md": {
	id: "tenkawa/2026-06-05-shoene-kaishu.md";
  slug: "tenkawa/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tenri/2026-05-12-taiyoko.md": {
	id: "tenri/2026-05-12-taiyoko.md";
  slug: "tenri/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tenri/2026-05-29-taishin-kaishu.md": {
	id: "tenri/2026-05-29-taishin-kaishu.md";
  slug: "tenri/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tenri/2026-06-05-shoene-kaishu.md": {
	id: "tenri/2026-06-05-shoene-kaishu.md";
  slug: "tenri/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tobishima/2026-05-10-shoene-aichi-pref.md": {
	id: "tobishima/2026-05-10-shoene-aichi-pref.md";
  slug: "tobishima/2026-05-10-shoene-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tobishima/2026-05-10-sogyo-aichi-pref.md": {
	id: "tobishima/2026-05-10-sogyo-aichi-pref.md";
  slug: "tobishima/2026-05-10-sogyo-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tobishima/2026-06-03-taishin-kaishu.md": {
	id: "tobishima/2026-06-03-taishin-kaishu.md";
  slug: "tobishima/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tochigi_city/2026-05-29-taishin-kaishu.md": {
	id: "tochigi_city/2026-05-29-taishin-kaishu.md";
  slug: "tochigi_city/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tochigi_city/2026-06-05-shoene-kaishu.md": {
	id: "tochigi_city/2026-06-05-shoene-kaishu.md";
  slug: "tochigi_city/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tochigi_pref/2026-05-27-taiyoko.md": {
	id: "tochigi_pref/2026-05-27-taiyoko.md";
  slug: "tochigi_pref/2026-05-27-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tochigi_pref/2026-06-03-shoene-kaishu.md": {
	id: "tochigi_pref/2026-06-03-shoene-kaishu.md";
  slug: "tochigi_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tochigi_pref/2026-06-03-taishin-kaishu.md": {
	id: "tochigi_pref/2026-06-03-taishin-kaishu.md";
  slug: "tochigi_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toda/2026-05-10-shoene.md": {
	id: "toda/2026-05-10-shoene.md";
  slug: "toda/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toda/2026-05-10-taiyoko.md": {
	id: "toda/2026-05-10-taiyoko.md";
  slug: "toda/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toda/2026-06-03-taishin-kaishu.md": {
	id: "toda/2026-06-03-taishin-kaishu.md";
  slug: "toda/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toei/2026-05-10-shoene-aichi-pref.md": {
	id: "toei/2026-05-10-shoene-aichi-pref.md";
  slug: "toei/2026-05-10-shoene-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toei/2026-05-10-sogyo-aichi-pref.md": {
	id: "toei/2026-05-10-sogyo-aichi-pref.md";
  slug: "toei/2026-05-10-sogyo-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toei/2026-06-03-taishin-kaishu.md": {
	id: "toei/2026-06-03-taishin-kaishu.md";
  slug: "toei/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"togo/2026-05-10-shoene-aichi-pref.md": {
	id: "togo/2026-05-10-shoene-aichi-pref.md";
  slug: "togo/2026-05-10-shoene-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"togo/2026-05-10-sogyo-aichi-pref.md": {
	id: "togo/2026-05-10-sogyo-aichi-pref.md";
  slug: "togo/2026-05-10-sogyo-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"togo/2026-06-03-taishin-kaishu.md": {
	id: "togo/2026-06-03-taishin-kaishu.md";
  slug: "togo/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokai_aichi/2026-05-10-jutaku-shoene.md": {
	id: "tokai_aichi/2026-05-10-jutaku-shoene.md";
  slug: "tokai_aichi/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokai_aichi/2026-05-10-sogyo-shien.md": {
	id: "tokai_aichi/2026-05-10-sogyo-shien.md";
  slug: "tokai_aichi/2026-05-10-sogyo-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokai_aichi/2026-05-29-taishin-kaishu.md": {
	id: "tokai_aichi/2026-05-29-taishin-kaishu.md";
  slug: "tokai_aichi/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokigawa/2026-05-10-shoene.md": {
	id: "tokigawa/2026-05-10-shoene.md";
  slug: "tokigawa/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokigawa/2026-05-10-taiyoko.md": {
	id: "tokigawa/2026-05-10-taiyoko.md";
  slug: "tokigawa/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokigawa/2026-06-03-taishin-kaishu.md": {
	id: "tokigawa/2026-06-03-taishin-kaishu.md";
  slug: "tokigawa/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokoname/2026-05-10-jutaku-reform.md": {
	id: "tokoname/2026-05-10-jutaku-reform.md";
  slug: "tokoname/2026-05-10-jutaku-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokoname/2026-05-10-jutaku-shoene.md": {
	id: "tokoname/2026-05-10-jutaku-shoene.md";
  slug: "tokoname/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokoname/2026-05-29-taishin-kaishu.md": {
	id: "tokoname/2026-05-29-taishin-kaishu.md";
  slug: "tokoname/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokorozawa/2026-05-10-non-fit.md": {
	id: "tokorozawa/2026-05-10-non-fit.md";
  slug: "tokorozawa/2026-05-10-non-fit";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokorozawa/2026-05-10-non_fit.md": {
	id: "tokorozawa/2026-05-10-non_fit.md";
  slug: "tokorozawa/2026-05-10-non_fit";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokorozawa/2026-05-10-smart-house.md": {
	id: "tokorozawa/2026-05-10-smart-house.md";
  slug: "tokorozawa/2026-05-10-smart-house";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokorozawa/2026-05-10-smart.md": {
	id: "tokorozawa/2026-05-10-smart.md";
  slug: "tokorozawa/2026-05-10-smart";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokorozawa/2026-05-29-taishin-kaishu.md": {
	id: "tokorozawa/2026-05-29-taishin-kaishu.md";
  slug: "tokorozawa/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokorozawa/2026-06-05-shoene-kaishu.md": {
	id: "tokorozawa/2026-06-05-shoene-kaishu.md";
  slug: "tokorozawa/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokushima/2026-05-27-taishin-kaishu.md": {
	id: "tokushima/2026-05-27-taishin-kaishu.md";
  slug: "tokushima/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokushima/2026-06-05-shoene-kaishu-pref.md": {
	id: "tokushima/2026-06-05-shoene-kaishu-pref.md";
  slug: "tokushima/2026-06-05-shoene-kaishu-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokushima/2026-06-05-shoene-kaishu.md": {
	id: "tokushima/2026-06-05-shoene-kaishu.md";
  slug: "tokushima/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokushima_pref/2026-05-27-taishin-kaishu.md": {
	id: "tokushima_pref/2026-05-27-taishin-kaishu.md";
  slug: "tokushima_pref/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokushima_pref/2026-05-27-taiyoko.md": {
	id: "tokushima_pref/2026-05-27-taiyoko.md";
  slug: "tokushima_pref/2026-05-27-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokushima_pref/2026-06-05-shoene-kaishu.md": {
	id: "tokushima_pref/2026-06-05-shoene-kaishu.md";
  slug: "tokushima_pref/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-05-dannetsu-solar.md": {
	id: "tokyo/2026-05-05-dannetsu-solar.md";
  slug: "tokyo/2026-05-05-dannetsu-solar";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-05-setsubi-yakushin.md": {
	id: "tokyo/2026-05-05-setsubi-yakushin.md";
  slug: "tokyo/2026-05-05-setsubi-yakushin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-05-sogyo-josei.md": {
	id: "tokyo/2026-05-05-sogyo-josei.md";
  slug: "tokyo/2026-05-05-sogyo-josei";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-05-zero-emi-jutaku.md": {
	id: "tokyo/2026-05-05-zero-emi-jutaku.md";
  slug: "tokyo/2026-05-05-zero-emi-jutaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-0pspqma2.md": {
	id: "tokyo/2026-05-17-jgrants-0pspqma2.md";
  slug: "tokyo/2026-05-17-jgrants-0pspqma2";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-5iqyxeaq.md": {
	id: "tokyo/2026-05-17-jgrants-5iqyxeaq.md";
  slug: "tokyo/2026-05-17-jgrants-5iqyxeaq";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-7crsaea0.md": {
	id: "tokyo/2026-05-17-jgrants-7crsaea0.md";
  slug: "tokyo/2026-05-17-jgrants-7crsaea0";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-7csteeaw.md": {
	id: "tokyo/2026-05-17-jgrants-7csteeaw.md";
  slug: "tokyo/2026-05-17-jgrants-7csteeaw";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-7cstoeaw.md": {
	id: "tokyo/2026-05-17-jgrants-7cstoeaw.md";
  slug: "tokyo/2026-05-17-jgrants-7cstoeaw";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-88tq4mam.md": {
	id: "tokyo/2026-05-17-jgrants-88tq4mam.md";
  slug: "tokyo/2026-05-17-jgrants-88tq4mam";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-8av8bmac.md": {
	id: "tokyo/2026-05-17-jgrants-8av8bmac.md";
  slug: "tokyo/2026-05-17-jgrants-8av8bmac";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdk76mah.md": {
	id: "tokyo/2026-05-17-jgrants-cdk76mah.md";
  slug: "tokyo/2026-05-17-jgrants-cdk76mah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdk7nmax.md": {
	id: "tokyo/2026-05-17-jgrants-cdk7nmax.md";
  slug: "tokyo/2026-05-17-jgrants-cdk7nmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdk7omax.md": {
	id: "tokyo/2026-05-17-jgrants-cdk7omax.md";
  slug: "tokyo/2026-05-17-jgrants-cdk7omax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdmqvmap.md": {
	id: "tokyo/2026-05-17-jgrants-cdmqvmap.md";
  slug: "tokyo/2026-05-17-jgrants-cdmqvmap";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdmssmap.md": {
	id: "tokyo/2026-05-17-jgrants-cdmssmap.md";
  slug: "tokyo/2026-05-17-jgrants-cdmssmap";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdniamah.md": {
	id: "tokyo/2026-05-17-jgrants-cdniamah.md";
  slug: "tokyo/2026-05-17-jgrants-cdniamah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdrmrmah.md": {
	id: "tokyo/2026-05-17-jgrants-cdrmrmah.md";
  slug: "tokyo/2026-05-17-jgrants-cdrmrmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdustmax.md": {
	id: "tokyo/2026-05-17-jgrants-cdustmax.md";
  slug: "tokyo/2026-05-17-jgrants-cdustmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cduvzmah.md": {
	id: "tokyo/2026-05-17-jgrants-cduvzmah.md";
  slug: "tokyo/2026-05-17-jgrants-cduvzmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdvdpmax.md": {
	id: "tokyo/2026-05-17-jgrants-cdvdpmax.md";
  slug: "tokyo/2026-05-17-jgrants-cdvdpmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdvnrmap.md": {
	id: "tokyo/2026-05-17-jgrants-cdvnrmap.md";
  slug: "tokyo/2026-05-17-jgrants-cdvnrmap";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdvxkmax.md": {
	id: "tokyo/2026-05-17-jgrants-cdvxkmax.md";
  slug: "tokyo/2026-05-17-jgrants-cdvxkmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdwngmah.md": {
	id: "tokyo/2026-05-17-jgrants-cdwngmah.md";
  slug: "tokyo/2026-05-17-jgrants-cdwngmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdx1kmax.md": {
	id: "tokyo/2026-05-17-jgrants-cdx1kmax.md";
  slug: "tokyo/2026-05-17-jgrants-cdx1kmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdxemmap.md": {
	id: "tokyo/2026-05-17-jgrants-cdxemmap.md";
  slug: "tokyo/2026-05-17-jgrants-cdxemmap";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdxmimap.md": {
	id: "tokyo/2026-05-17-jgrants-cdxmimap.md";
  slug: "tokyo/2026-05-17-jgrants-cdxmimap";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdy6vmah.md": {
	id: "tokyo/2026-05-17-jgrants-cdy6vmah.md";
  slug: "tokyo/2026-05-17-jgrants-cdy6vmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdybimap.md": {
	id: "tokyo/2026-05-17-jgrants-cdybimap.md";
  slug: "tokyo/2026-05-17-jgrants-cdybimap";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdycumah.md": {
	id: "tokyo/2026-05-17-jgrants-cdycumah.md";
  slug: "tokyo/2026-05-17-jgrants-cdycumah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdydfmah.md": {
	id: "tokyo/2026-05-17-jgrants-cdydfmah.md";
  slug: "tokyo/2026-05-17-jgrants-cdydfmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdydgmah.md": {
	id: "tokyo/2026-05-17-jgrants-cdydgmah.md";
  slug: "tokyo/2026-05-17-jgrants-cdydgmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdydhmah.md": {
	id: "tokyo/2026-05-17-jgrants-cdydhmah.md";
  slug: "tokyo/2026-05-17-jgrants-cdydhmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdyk9mah.md": {
	id: "tokyo/2026-05-17-jgrants-cdyk9mah.md";
  slug: "tokyo/2026-05-17-jgrants-cdyk9mah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdykamax.md": {
	id: "tokyo/2026-05-17-jgrants-cdykamax.md";
  slug: "tokyo/2026-05-17-jgrants-cdykamax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdykbmax.md": {
	id: "tokyo/2026-05-17-jgrants-cdykbmax.md";
  slug: "tokyo/2026-05-17-jgrants-cdykbmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdynumah.md": {
	id: "tokyo/2026-05-17-jgrants-cdynumah.md";
  slug: "tokyo/2026-05-17-jgrants-cdynumah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdyqimap.md": {
	id: "tokyo/2026-05-17-jgrants-cdyqimap.md";
  slug: "tokyo/2026-05-17-jgrants-cdyqimap";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdyqnmap.md": {
	id: "tokyo/2026-05-17-jgrants-cdyqnmap.md";
  slug: "tokyo/2026-05-17-jgrants-cdyqnmap";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdyqomap.md": {
	id: "tokyo/2026-05-17-jgrants-cdyqomap.md";
  slug: "tokyo/2026-05-17-jgrants-cdyqomap";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdyr2map.md": {
	id: "tokyo/2026-05-17-jgrants-cdyr2map.md";
  slug: "tokyo/2026-05-17-jgrants-cdyr2map";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdyrcma5.md": {
	id: "tokyo/2026-05-17-jgrants-cdyrcma5.md";
  slug: "tokyo/2026-05-17-jgrants-cdyrcma5";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdyrhma5.md": {
	id: "tokyo/2026-05-17-jgrants-cdyrhma5.md";
  slug: "tokyo/2026-05-17-jgrants-cdyrhma5";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdyukmap.md": {
	id: "tokyo/2026-05-17-jgrants-cdyukmap.md";
  slug: "tokyo/2026-05-17-jgrants-cdyukmap";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-cdyummap.md": {
	id: "tokyo/2026-05-17-jgrants-cdyummap.md";
  slug: "tokyo/2026-05-17-jgrants-cdyummap";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-ubqnseav.md": {
	id: "tokyo/2026-05-17-jgrants-ubqnseav.md";
  slug: "tokyo/2026-05-17-jgrants-ubqnseav";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-ubqnxeav.md": {
	id: "tokyo/2026-05-17-jgrants-ubqnxeav.md";
  slug: "tokyo/2026-05-17-jgrants-ubqnxeav";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-17-jgrants-zuexceaj.md": {
	id: "tokyo/2026-05-17-jgrants-zuexceaj.md";
  slug: "tokyo/2026-05-17-jgrants-zuexceaj";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-20-jgrants-cdykimah.md": {
	id: "tokyo/2026-05-20-jgrants-cdykimah.md";
  slug: "tokyo/2026-05-20-jgrants-cdykimah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-05-20-jgrants-cdytima5.md": {
	id: "tokyo/2026-05-20-jgrants-cdytima5.md";
  slug: "tokyo/2026-05-20-jgrants-cdytima5";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-06-03-shoene-kaishu.md": {
	id: "tokyo/2026-06-03-shoene-kaishu.md";
  slug: "tokyo/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tokyo/2026-06-03-taishin-kaishu.md": {
	id: "tokyo/2026-06-03-taishin-kaishu.md";
  slug: "tokyo/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tondabayashi/2026-05-03-akitenpo.md": {
	id: "tondabayashi/2026-05-03-akitenpo.md";
  slug: "tondabayashi/2026-05-03-akitenpo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tondabayashi/2026-05-03-shoko-shinko.md": {
	id: "tondabayashi/2026-05-03-shoko-shinko.md";
  slug: "tondabayashi/2026-05-03-shoko-shinko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tondabayashi/2026-06-03-taishin-kaishu.md": {
	id: "tondabayashi/2026-06-03-taishin-kaishu.md";
  slug: "tondabayashi/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tondabayashi/2026-06-05-shoene-kaishu.md": {
	id: "tondabayashi/2026-06-05-shoene-kaishu.md";
  slug: "tondabayashi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toshima/2026-05-05-eco-jutaku.md": {
	id: "toshima/2026-05-05-eco-jutaku.md";
  slug: "toshima/2026-05-05-eco-jutaku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toshima/2026-05-29-taishin-kaishu.md": {
	id: "toshima/2026-05-29-taishin-kaishu.md";
  slug: "toshima/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toshima/2026-06-05-shoene-kaishu.md": {
	id: "toshima/2026-06-05-shoene-kaishu.md";
  slug: "toshima/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"totsukawa/2026-05-12-iju.md": {
	id: "totsukawa/2026-05-12-iju.md";
  slug: "totsukawa/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"totsukawa/2026-06-03-taishin-kaishu.md": {
	id: "totsukawa/2026-06-03-taishin-kaishu.md";
  slug: "totsukawa/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"totsukawa/2026-06-05-shoene-kaishu.md": {
	id: "totsukawa/2026-06-05-shoene-kaishu.md";
  slug: "totsukawa/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tottori/2026-05-27-taishin-kaishu.md": {
	id: "tottori/2026-05-27-taishin-kaishu.md";
  slug: "tottori/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tottori/2026-06-05-shoene-hojokin.md": {
	id: "tottori/2026-06-05-shoene-hojokin.md";
  slug: "tottori/2026-06-05-shoene-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tottori/2026-06-05-shoene-kaishu.md": {
	id: "tottori/2026-06-05-shoene-kaishu.md";
  slug: "tottori/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tottori_pref/2026-05-17-jgrants-cdynkmah.md": {
	id: "tottori_pref/2026-05-17-jgrants-cdynkmah.md";
  slug: "tottori_pref/2026-05-17-jgrants-cdynkmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tottori_pref/2026-06-03-shoene-kaishu.md": {
	id: "tottori_pref/2026-06-03-shoene-kaishu.md";
  slug: "tottori_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tottori_pref/2026-06-03-taishin-kaishu.md": {
	id: "tottori_pref/2026-06-03-taishin-kaishu.md";
  slug: "tottori_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyama/2026-05-27-iju-shienkin.md": {
	id: "toyama/2026-05-27-iju-shienkin.md";
  slug: "toyama/2026-05-27-iju-shienkin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyama/2026-05-27-taishin-kaishu.md": {
	id: "toyama/2026-05-27-taishin-kaishu.md";
  slug: "toyama/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyama/2026-05-27-taiyoko.md": {
	id: "toyama/2026-05-27-taiyoko.md";
  slug: "toyama/2026-05-27-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyama/2026-06-05-shoene-kaishu.md": {
	id: "toyama/2026-06-05-shoene-kaishu.md";
  slug: "toyama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyama/2026-06-05-shoene-kiuki.md": {
	id: "toyama/2026-06-05-shoene-kiuki.md";
  slug: "toyama/2026-06-05-shoene-kiuki";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyama_pref/2026-05-17-jgrants-cdyinmax.md": {
	id: "toyama_pref/2026-05-17-jgrants-cdyinmax.md";
  slug: "toyama_pref/2026-05-17-jgrants-cdyinmax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyama_pref/2026-06-03-shoene-kaishu.md": {
	id: "toyama_pref/2026-06-03-shoene-kaishu.md";
  slug: "toyama_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyama_pref/2026-06-03-taishin-kaishu.md": {
	id: "toyama_pref/2026-06-03-taishin-kaishu.md";
  slug: "toyama_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyoake/2026-05-10-jutaku-reform.md": {
	id: "toyoake/2026-05-10-jutaku-reform.md";
  slug: "toyoake/2026-05-10-jutaku-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyoake/2026-05-10-jutaku-shoene.md": {
	id: "toyoake/2026-05-10-jutaku-shoene.md";
  slug: "toyoake/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyoake/2026-06-03-taishin-kaishu.md": {
	id: "toyoake/2026-06-03-taishin-kaishu.md";
  slug: "toyoake/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyohashi/2026-05-10-jutaku-shoene.md": {
	id: "toyohashi/2026-05-10-jutaku-shoene.md";
  slug: "toyohashi/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyohashi/2026-05-10-sangyo-shinko.md": {
	id: "toyohashi/2026-05-10-sangyo-shinko.md";
  slug: "toyohashi/2026-05-10-sangyo-shinko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyohashi/2026-05-10-sogyo-shien.md": {
	id: "toyohashi/2026-05-10-sogyo-shien.md";
  slug: "toyohashi/2026-05-10-sogyo-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyohashi/2026-05-29-taishin-kaishu.md": {
	id: "toyohashi/2026-05-29-taishin-kaishu.md";
  slug: "toyohashi/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyohashi/2026-06-05-energy-hojokin.md": {
	id: "toyohashi/2026-06-05-energy-hojokin.md";
  slug: "toyohashi/2026-06-05-energy-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyokawa/2026-05-10-jutaku-shoene.md": {
	id: "toyokawa/2026-05-10-jutaku-shoene.md";
  slug: "toyokawa/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyokawa/2026-05-10-sogyo-shien.md": {
	id: "toyokawa/2026-05-10-sogyo-shien.md";
  slug: "toyokawa/2026-05-10-sogyo-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyokawa/2026-05-29-taishin-kaishu.md": {
	id: "toyokawa/2026-05-29-taishin-kaishu.md";
  slug: "toyokawa/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyonaka/2026-04-29-aiseminar.md": {
	id: "toyonaka/2026-04-29-aiseminar.md";
  slug: "toyonaka/2026-04-29-aiseminar";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyonaka/2026-04-29-aisokushin.md": {
	id: "toyonaka/2026-04-29-aisokushin.md";
  slug: "toyonaka/2026-04-29-aisokushin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyonaka/2026-04-29-itkasokushin.md": {
	id: "toyonaka/2026-04-29-itkasokushin.md";
  slug: "toyonaka/2026-04-29-itkasokushin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyonaka/2026-04-29-jinzaikakuho.md": {
	id: "toyonaka/2026-04-29-jinzaikakuho.md";
  slug: "toyonaka/2026-04-29-jinzaikakuho";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyonaka/2026-04-29-keieijinzai.md": {
	id: "toyonaka/2026-04-29-keieijinzai.md";
  slug: "toyonaka/2026-04-29-keieijinzai";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyonaka/2026-04-29-kigyoukasoushutu.md": {
	id: "toyonaka/2026-04-29-kigyoukasoushutu.md";
  slug: "toyonaka/2026-04-29-kigyoukasoushutu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyonaka/2026-04-29-koufukakachika.md": {
	id: "toyonaka/2026-04-29-koufukakachika.md";
  slug: "toyonaka/2026-04-29-koufukakachika";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyonaka/2026-04-29-tenjikai.md": {
	id: "toyonaka/2026-04-29-tenjikai.md";
  slug: "toyonaka/2026-04-29-tenjikai";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyonaka/2026-04-29-tourokumenkyozei.md": {
	id: "toyonaka/2026-04-29-tourokumenkyozei.md";
  slug: "toyonaka/2026-04-29-tourokumenkyozei";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyonaka/2026-04-29-uriageup.md": {
	id: "toyonaka/2026-04-29-uriageup.md";
  slug: "toyonaka/2026-04-29-uriageup";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyonaka/2026-05-29-taishin-kaishu.md": {
	id: "toyonaka/2026-05-29-taishin-kaishu.md";
  slug: "toyonaka/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyonaka/2026-06-05-shoene-kaishu.md": {
	id: "toyonaka/2026-06-05-shoene-kaishu.md";
  slug: "toyonaka/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyono/2026-05-04-akiya-reform.md": {
	id: "toyono/2026-05-04-akiya-reform.md";
  slug: "toyono/2026-05-04-akiya-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyono/2026-05-04-nigiwai.md": {
	id: "toyono/2026-05-04-nigiwai.md";
  slug: "toyono/2026-05-04-nigiwai";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyono/2026-06-03-taishin-kaishu.md": {
	id: "toyono/2026-06-03-taishin-kaishu.md";
  slug: "toyono/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyono/2026-06-05-shoene-kaishu.md": {
	id: "toyono/2026-06-05-shoene-kaishu.md";
  slug: "toyono/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyooka/2026-05-11-sogyo.md": {
	id: "toyooka/2026-05-11-sogyo.md";
  slug: "toyooka/2026-05-11-sogyo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyooka/2026-05-11-taiyoko.md": {
	id: "toyooka/2026-05-11-taiyoko.md";
  slug: "toyooka/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyooka/2026-05-29-taishin-kaishu.md": {
	id: "toyooka/2026-05-29-taishin-kaishu.md";
  slug: "toyooka/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyooka/2026-06-05-shoene-kaishu.md": {
	id: "toyooka/2026-06-05-shoene-kaishu.md";
  slug: "toyooka/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyosato/2026-05-12-taiyoko.md": {
	id: "toyosato/2026-05-12-taiyoko.md";
  slug: "toyosato/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyosato/2026-05-29-taishin-kaishu.md": {
	id: "toyosato/2026-05-29-taishin-kaishu.md";
  slug: "toyosato/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyosato/2026-06-05-shoene-kaishu.md": {
	id: "toyosato/2026-06-05-shoene-kaishu.md";
  slug: "toyosato/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyota/2026-05-10-ev-suiso.md": {
	id: "toyota/2026-05-10-ev-suiso.md";
  slug: "toyota/2026-05-10-ev-suiso";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyota/2026-05-10-jutaku-shoene.md": {
	id: "toyota/2026-05-10-jutaku-shoene.md";
  slug: "toyota/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyota/2026-05-10-sogyo-shien.md": {
	id: "toyota/2026-05-10-sogyo-shien.md";
  slug: "toyota/2026-05-10-sogyo-shien";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyota/2026-05-29-taishin-kaishu.md": {
	id: "toyota/2026-05-29-taishin-kaishu.md";
  slug: "toyota/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyota/2026-06-05-shoene-kaishu-hojo.md": {
	id: "toyota/2026-06-05-shoene-kaishu-hojo.md";
  slug: "toyota/2026-06-05-shoene-kaishu-hojo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyoyama/2026-05-10-shoene-aichi-pref.md": {
	id: "toyoyama/2026-05-10-shoene-aichi-pref.md";
  slug: "toyoyama/2026-05-10-shoene-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyoyama/2026-05-10-sogyo-aichi-pref.md": {
	id: "toyoyama/2026-05-10-sogyo-aichi-pref.md";
  slug: "toyoyama/2026-05-10-sogyo-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"toyoyama/2026-06-03-taishin-kaishu.md": {
	id: "toyoyama/2026-06-03-taishin-kaishu.md";
  slug: "toyoyama/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tsu/2026-05-27-taishin-kaishu.md": {
	id: "tsu/2026-05-27-taishin-kaishu.md";
  slug: "tsu/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tsu/2026-05-27-taiyoko.md": {
	id: "tsu/2026-05-27-taiyoko.md";
  slug: "tsu/2026-05-27-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tsu/2026-06-05-shoene-kaishu.md": {
	id: "tsu/2026-06-05-shoene-kaishu.md";
  slug: "tsu/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tsukuba/2026-05-29-taishin-kaishu.md": {
	id: "tsukuba/2026-05-29-taishin-kaishu.md";
  slug: "tsukuba/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tsukuba/2026-06-05-shoene-kaishu.md": {
	id: "tsukuba/2026-06-05-shoene-kaishu.md";
  slug: "tsukuba/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tsurugashima/2026-05-10-shoene.md": {
	id: "tsurugashima/2026-05-10-shoene.md";
  slug: "tsurugashima/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tsurugashima/2026-05-10-taiyoko.md": {
	id: "tsurugashima/2026-05-10-taiyoko.md";
  slug: "tsurugashima/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tsurugashima/2026-06-03-taishin-kaishu.md": {
	id: "tsurugashima/2026-06-03-taishin-kaishu.md";
  slug: "tsurugashima/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tsuruoka/2026-05-29-taishin-kaishu.md": {
	id: "tsuruoka/2026-05-29-taishin-kaishu.md";
  slug: "tsuruoka/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tsuruoka/2026-06-05-shoene-kaishu.md": {
	id: "tsuruoka/2026-06-05-shoene-kaishu.md";
  slug: "tsuruoka/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tsushima/2026-05-10-jutaku-reform.md": {
	id: "tsushima/2026-05-10-jutaku-reform.md";
  slug: "tsushima/2026-05-10-jutaku-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tsushima/2026-05-10-jutaku-shoene.md": {
	id: "tsushima/2026-05-10-jutaku-shoene.md";
  slug: "tsushima/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tsushima/2026-06-03-taishin-kaishu.md": {
	id: "tsushima/2026-06-03-taishin-kaishu.md";
  slug: "tsushima/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tyone/2026-05-10-shoene-aichi-pref.md": {
	id: "tyone/2026-05-10-shoene-aichi-pref.md";
  slug: "tyone/2026-05-10-shoene-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tyone/2026-05-10-sogyo-aichi-pref.md": {
	id: "tyone/2026-05-10-sogyo-aichi-pref.md";
  slug: "tyone/2026-05-10-sogyo-aichi-pref";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"tyone/2026-06-03-taishin-kaishu.md": {
	id: "tyone/2026-06-03-taishin-kaishu.md";
  slug: "tyone/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ube/2026-05-29-taishin-kaishu.md": {
	id: "ube/2026-05-29-taishin-kaishu.md";
  slug: "ube/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ube/2026-06-05-shoene-kaishu.md": {
	id: "ube/2026-06-05-shoene-kaishu.md";
  slug: "ube/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"uda/2026-05-12-iju.md": {
	id: "uda/2026-05-12-iju.md";
  slug: "uda/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"uda/2026-05-12-taiyoko.md": {
	id: "uda/2026-05-12-taiyoko.md";
  slug: "uda/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"uda/2026-05-29-taishin-kaishu.md": {
	id: "uda/2026-05-29-taishin-kaishu.md";
  slug: "uda/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"uda/2026-06-05-shoene-kaishu.md": {
	id: "uda/2026-06-05-shoene-kaishu.md";
  slug: "uda/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ueda/2026-05-29-taishin-kaishu.md": {
	id: "ueda/2026-05-29-taishin-kaishu.md";
  slug: "ueda/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ueda/2026-06-05-shoene-kaishu.md": {
	id: "ueda/2026-06-05-shoene-kaishu.md";
  slug: "ueda/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"uji/2026-05-11-taiyoko.md": {
	id: "uji/2026-05-11-taiyoko.md";
  slug: "uji/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"uji/2026-05-29-taishin-kaishu.md": {
	id: "uji/2026-05-29-taishin-kaishu.md";
  slug: "uji/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"uji/2026-06-05-shoene-kaishu.md": {
	id: "uji/2026-06-05-shoene-kaishu.md";
  slug: "uji/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ujitawara/2026-05-11-iju.md": {
	id: "ujitawara/2026-05-11-iju.md";
  slug: "ujitawara/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ujitawara/2026-05-11-taiyoko.md": {
	id: "ujitawara/2026-05-11-taiyoko.md";
  slug: "ujitawara/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ujitawara/2026-06-03-taishin-kaishu.md": {
	id: "ujitawara/2026-06-03-taishin-kaishu.md";
  slug: "ujitawara/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"ujitawara/2026-06-05-shoene-kaishu.md": {
	id: "ujitawara/2026-06-05-shoene-kaishu.md";
  slug: "ujitawara/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"urayasu/2026-05-29-datanso.md": {
	id: "urayasu/2026-05-29-datanso.md";
  slug: "urayasu/2026-05-29-datanso";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"urayasu/2026-06-03-taishin-kaishu.md": {
	id: "urayasu/2026-06-03-taishin-kaishu.md";
  slug: "urayasu/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"urayasu/2026-06-05-shoene-kaishu.md": {
	id: "urayasu/2026-06-05-shoene-kaishu.md";
  slug: "urayasu/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"utsunomiya/2026-05-24-chusho-yushi-sogyo.md": {
	id: "utsunomiya/2026-05-24-chusho-yushi-sogyo.md";
  slug: "utsunomiya/2026-05-24-chusho-yushi-sogyo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"utsunomiya/2026-05-29-taishin-kaishu.md": {
	id: "utsunomiya/2026-05-29-taishin-kaishu.md";
  slug: "utsunomiya/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"utsunomiya/2026-06-05-shoene-kaishu.md": {
	id: "utsunomiya/2026-06-05-shoene-kaishu.md";
  slug: "utsunomiya/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"utsunomiya/2026-06-05-shoene-zeh.md": {
	id: "utsunomiya/2026-06-05-shoene-zeh.md";
  slug: "utsunomiya/2026-06-05-shoene-zeh";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"wakayama/2026-05-12-kosodate.md": {
	id: "wakayama/2026-05-12-kosodate.md";
  slug: "wakayama/2026-05-12-kosodate";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"wakayama/2026-05-12-taiyoko.md": {
	id: "wakayama/2026-05-12-taiyoko.md";
  slug: "wakayama/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"wakayama/2026-05-29-taishin-kaishu.md": {
	id: "wakayama/2026-05-29-taishin-kaishu.md";
  slug: "wakayama/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"wakayama/2026-06-05-shoene-2026.md": {
	id: "wakayama/2026-06-05-shoene-2026.md";
  slug: "wakayama/2026-06-05-shoene-2026";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"wakayama/2026-06-05-shoene-kaishu.md": {
	id: "wakayama/2026-06-05-shoene-kaishu.md";
  slug: "wakayama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"wakayama_pref/2026-05-12-iju.md": {
	id: "wakayama_pref/2026-05-12-iju.md";
  slug: "wakayama_pref/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"wakayama_pref/2026-05-12-taiyoko.md": {
	id: "wakayama_pref/2026-05-12-taiyoko.md";
  slug: "wakayama_pref/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"wakayama_pref/2026-05-17-jgrants-cdynzmah.md": {
	id: "wakayama_pref/2026-05-17-jgrants-cdynzmah.md";
  slug: "wakayama_pref/2026-05-17-jgrants-cdynzmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"wakayama_pref/2026-06-03-shoene-kaishu.md": {
	id: "wakayama_pref/2026-06-03-shoene-kaishu.md";
  slug: "wakayama_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"wakayama_pref/2026-06-03-taishin-kaishu.md": {
	id: "wakayama_pref/2026-06-03-taishin-kaishu.md";
  slug: "wakayama_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"wako/2026-05-10-shoene.md": {
	id: "wako/2026-05-10-shoene.md";
  slug: "wako/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"wako/2026-05-10-taiyoko.md": {
	id: "wako/2026-05-10-taiyoko.md";
  slug: "wako/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"wako/2026-06-03-taishin-kaishu.md": {
	id: "wako/2026-06-03-taishin-kaishu.md";
  slug: "wako/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"warabi/2026-05-10-shoene.md": {
	id: "warabi/2026-05-10-shoene.md";
  slug: "warabi/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"warabi/2026-05-10-taiyoko.md": {
	id: "warabi/2026-05-10-taiyoko.md";
  slug: "warabi/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"warabi/2026-06-03-taishin-kaishu.md": {
	id: "warabi/2026-06-03-taishin-kaishu.md";
  slug: "warabi/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"wazuka/2026-05-11-iju.md": {
	id: "wazuka/2026-05-11-iju.md";
  slug: "wazuka/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"wazuka/2026-05-11-taiyoko.md": {
	id: "wazuka/2026-05-11-taiyoko.md";
  slug: "wazuka/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"wazuka/2026-06-03-taishin-kaishu.md": {
	id: "wazuka/2026-06-03-taishin-kaishu.md";
  slug: "wazuka/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"wazuka/2026-06-05-shoene-kaishu.md": {
	id: "wazuka/2026-06-05-shoene-kaishu.md";
  slug: "wazuka/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yabu/2026-05-11-iju.md": {
	id: "yabu/2026-05-11-iju.md";
  slug: "yabu/2026-05-11-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yabu/2026-05-11-taiyoko.md": {
	id: "yabu/2026-05-11-taiyoko.md";
  slug: "yabu/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yabu/2026-05-29-taishin-kaishu.md": {
	id: "yabu/2026-05-29-taishin-kaishu.md";
  slug: "yabu/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yabu/2026-06-05-shoene-kaishu.md": {
	id: "yabu/2026-06-05-shoene-kaishu.md";
  slug: "yabu/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamagata/2026-05-23-chusho-yushi.md": {
	id: "yamagata/2026-05-23-chusho-yushi.md";
  slug: "yamagata/2026-05-23-chusho-yushi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamagata/2026-05-25-ijuu-shienkin.md": {
	id: "yamagata/2026-05-25-ijuu-shienkin.md";
  slug: "yamagata/2026-05-25-ijuu-shienkin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamagata/2026-05-29-taishin-kaishu.md": {
	id: "yamagata/2026-05-29-taishin-kaishu.md";
  slug: "yamagata/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamagata/2026-06-05-shoene-kaishu.md": {
	id: "yamagata/2026-06-05-shoene-kaishu.md";
  slug: "yamagata/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamagata/2026-06-05-shoene-setsubi.md": {
	id: "yamagata/2026-06-05-shoene-setsubi.md";
  slug: "yamagata/2026-06-05-shoene-setsubi";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamagata_pref/2026-05-17-jgrants-cdzdimax.md": {
	id: "yamagata_pref/2026-05-17-jgrants-cdzdimax.md";
  slug: "yamagata_pref/2026-05-17-jgrants-cdzdimax";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamagata_pref/2026-06-03-shoene-kaishu.md": {
	id: "yamagata_pref/2026-06-03-shoene-kaishu.md";
  slug: "yamagata_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamagata_pref/2026-06-03-taishin-kaishu.md": {
	id: "yamagata_pref/2026-06-03-taishin-kaishu.md";
  slug: "yamagata_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamaguchi/2026-05-27-taishin-kaishu.md": {
	id: "yamaguchi/2026-05-27-taishin-kaishu.md";
  slug: "yamaguchi/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamaguchi/2026-06-05-shoene-kaishu.md": {
	id: "yamaguchi/2026-06-05-shoene-kaishu.md";
  slug: "yamaguchi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamaguchi_pref/2026-05-17-jgrants-cdydnmah.md": {
	id: "yamaguchi_pref/2026-05-17-jgrants-cdydnmah.md";
  slug: "yamaguchi_pref/2026-05-17-jgrants-cdydnmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamaguchi_pref/2026-05-17-jgrants-cdydsmah.md": {
	id: "yamaguchi_pref/2026-05-17-jgrants-cdydsmah.md";
  slug: "yamaguchi_pref/2026-05-17-jgrants-cdydsmah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamaguchi_pref/2026-05-17-jgrants-cdyq7mah.md": {
	id: "yamaguchi_pref/2026-05-17-jgrants-cdyq7mah.md";
  slug: "yamaguchi_pref/2026-05-17-jgrants-cdyq7mah";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamaguchi_pref/2026-06-03-shoene-kaishu.md": {
	id: "yamaguchi_pref/2026-06-03-shoene-kaishu.md";
  slug: "yamaguchi_pref/2026-06-03-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamaguchi_pref/2026-06-03-taishin-kaishu.md": {
	id: "yamaguchi_pref/2026-06-03-taishin-kaishu.md";
  slug: "yamaguchi_pref/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamakita/2026-05-08-taiyoko-hojo.md": {
	id: "yamakita/2026-05-08-taiyoko-hojo.md";
  slug: "yamakita/2026-05-08-taiyoko-hojo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamakita/2026-05-12-iju.md": {
	id: "yamakita/2026-05-12-iju.md";
  slug: "yamakita/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamakita/2026-06-03-taishin-kaishu.md": {
	id: "yamakita/2026-06-03-taishin-kaishu.md";
  slug: "yamakita/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamakita/2026-06-05-shoene-kaishu.md": {
	id: "yamakita/2026-06-05-shoene-kaishu.md";
  slug: "yamakita/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamanashi_pref/2026-05-27-iju-shienkin.md": {
	id: "yamanashi_pref/2026-05-27-iju-shienkin.md";
  slug: "yamanashi_pref/2026-05-27-iju-shienkin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamanashi_pref/2026-05-27-taishin-kaishu.md": {
	id: "yamanashi_pref/2026-05-27-taishin-kaishu.md";
  slug: "yamanashi_pref/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamanashi_pref/2026-06-05-shoene-kaishu.md": {
	id: "yamanashi_pref/2026-06-05-shoene-kaishu.md";
  slug: "yamanashi_pref/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamato/2026-05-08-jikashouhi-taiyoko.md": {
	id: "yamato/2026-05-08-jikashouhi-taiyoko.md";
  slug: "yamato/2026-05-08-jikashouhi-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamato/2026-05-12-iju.md": {
	id: "yamato/2026-05-12-iju.md";
  slug: "yamato/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamato/2026-05-29-taishin-kaishu.md": {
	id: "yamato/2026-05-29-taishin-kaishu.md";
  slug: "yamato/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamato/2026-06-05-shoene-kaishu.md": {
	id: "yamato/2026-06-05-shoene-kaishu.md";
  slug: "yamato/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamatokoriyama/2026-05-12-taiyoko.md": {
	id: "yamatokoriyama/2026-05-12-taiyoko.md";
  slug: "yamatokoriyama/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamatokoriyama/2026-05-29-taishin-kaishu.md": {
	id: "yamatokoriyama/2026-05-29-taishin-kaishu.md";
  slug: "yamatokoriyama/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamatokoriyama/2026-06-05-shoene-kaishu.md": {
	id: "yamatokoriyama/2026-06-05-shoene-kaishu.md";
  slug: "yamatokoriyama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamatotakada/2026-05-12-taiyoko.md": {
	id: "yamatotakada/2026-05-12-taiyoko.md";
  slug: "yamatotakada/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamatotakada/2026-06-03-taishin-kaishu.md": {
	id: "yamatotakada/2026-06-03-taishin-kaishu.md";
  slug: "yamatotakada/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yamatotakada/2026-06-05-shoene-kaishu.md": {
	id: "yamatotakada/2026-06-05-shoene-kaishu.md";
  slug: "yamatotakada/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yao/2026-05-01-iyoku-hojokin.md": {
	id: "yao/2026-05-01-iyoku-hojokin.md";
  slug: "yao/2026-05-01-iyoku-hojokin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yao/2026-05-01-myhome.md": {
	id: "yao/2026-05-01-myhome.md";
  slug: "yao/2026-05-01-myhome";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yao/2026-05-01-taishin.md": {
	id: "yao/2026-05-01-taishin.md";
  slug: "yao/2026-05-01-taishin";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yao/2026-06-05-shoene-kaishu.md": {
	id: "yao/2026-06-05-shoene-kaishu.md";
  slug: "yao/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yashio/2026-05-10-shoene.md": {
	id: "yashio/2026-05-10-shoene.md";
  slug: "yashio/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yashio/2026-05-10-taiyoko.md": {
	id: "yashio/2026-05-10-taiyoko.md";
  slug: "yashio/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yashio/2026-06-03-taishin-kaishu.md": {
	id: "yashio/2026-06-03-taishin-kaishu.md";
  slug: "yashio/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yasu/2026-05-12-taiyoko.md": {
	id: "yasu/2026-05-12-taiyoko.md";
  slug: "yasu/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yasu/2026-05-29-taishin-kaishu.md": {
	id: "yasu/2026-05-29-taishin-kaishu.md";
  slug: "yasu/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yasu/2026-06-05-shoene-kaishu.md": {
	id: "yasu/2026-06-05-shoene-kaishu.md";
  slug: "yasu/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yatomi/2026-05-10-jutaku-reform.md": {
	id: "yatomi/2026-05-10-jutaku-reform.md";
  slug: "yatomi/2026-05-10-jutaku-reform";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yatomi/2026-05-10-jutaku-shoene.md": {
	id: "yatomi/2026-05-10-jutaku-shoene.md";
  slug: "yatomi/2026-05-10-jutaku-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yatomi/2026-06-03-taishin-kaishu.md": {
	id: "yatomi/2026-06-03-taishin-kaishu.md";
  slug: "yatomi/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yatsushiro/2026-05-29-taishin-kaishu.md": {
	id: "yatsushiro/2026-05-29-taishin-kaishu.md";
  slug: "yatsushiro/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yatsushiro/2026-06-05-shoene-kaishu.md": {
	id: "yatsushiro/2026-06-05-shoene-kaishu.md";
  slug: "yatsushiro/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yawata/2026-05-11-taiyoko.md": {
	id: "yawata/2026-05-11-taiyoko.md";
  slug: "yawata/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yawata/2026-05-29-taishin-kaishu.md": {
	id: "yawata/2026-05-29-taishin-kaishu.md";
  slug: "yawata/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yawata/2026-06-05-shoene-kaishu.md": {
	id: "yawata/2026-06-05-shoene-kaishu.md";
  slug: "yawata/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokkaichi/2026-05-29-taishin-kaishu.md": {
	id: "yokkaichi/2026-05-29-taishin-kaishu.md";
  slug: "yokkaichi/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokkaichi/2026-06-05-shoene-kaishu.md": {
	id: "yokkaichi/2026-06-05-shoene-kaishu.md";
  slug: "yokkaichi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokohama/2026-05-08-taiyoko-chusho.md": {
	id: "yokohama/2026-05-08-taiyoko-chusho.md";
  slug: "yokohama/2026-05-08-taiyoko-chusho";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokohama/2026-05-12-chusho.md": {
	id: "yokohama/2026-05-12-chusho.md";
  slug: "yokohama/2026-05-12-chusho";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokohama/2026-05-12-kosodatesumai.md": {
	id: "yokohama/2026-05-12-kosodatesumai.md";
  slug: "yokohama/2026-05-12-kosodatesumai";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokohama/2026-05-18-chizai-josei.md": {
	id: "yokohama/2026-05-18-chizai-josei.md";
  slug: "yokohama/2026-05-18-chizai-josei";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokohama/2026-05-18-fubunka-hojo.md": {
	id: "yokohama/2026-05-18-fubunka-hojo.md";
  slug: "yokohama/2026-05-18-fubunka-hojo";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokohama/2026-05-18-kosodate-chintai.md": {
	id: "yokohama/2026-05-18-kosodate-chintai.md";
  slug: "yokohama/2026-05-18-kosodate-chintai";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokohama/2026-05-18-led-chusho.md": {
	id: "yokohama/2026-05-18-led-chusho.md";
  slug: "yokohama/2026-05-18-led-chusho";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokohama/2026-05-18-led-rental.md": {
	id: "yokohama/2026-05-18-led-rental.md";
  slug: "yokohama/2026-05-18-led-rental";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokohama/2026-05-18-monozukuri-miryoku.md": {
	id: "yokohama/2026-05-18-monozukuri-miryoku.md";
  slug: "yokohama/2026-05-18-monozukuri-miryoku";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokohama/2026-05-18-shingi-kaihatsu.md": {
	id: "yokohama/2026-05-18-shingi-kaihatsu.md";
  slug: "yokohama/2026-05-18-shingi-kaihatsu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokohama/2026-05-18-shoene-kani.md": {
	id: "yokohama/2026-05-18-shoene-kani.md";
  slug: "yokohama/2026-05-18-shoene-kani";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokohama/2026-05-18-shoene-shindan.md": {
	id: "yokohama/2026-05-18-shoene-shindan.md";
  slug: "yokohama/2026-05-18-shoene-shindan";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokohama/2026-05-18-taiyoko-chusho.md": {
	id: "yokohama/2026-05-18-taiyoko-chusho.md";
  slug: "yokohama/2026-05-18-taiyoko-chusho";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokohama/2026-05-18-tenjikai-josei.md": {
	id: "yokohama/2026-05-18-tenjikai-josei.md";
  slug: "yokohama/2026-05-18-tenjikai-josei";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokohama/2026-05-24-dannetsu-energy.md": {
	id: "yokohama/2026-05-24-dannetsu-energy.md";
  slug: "yokohama/2026-05-24-dannetsu-energy";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokohama/2026-05-27-taishin-kaishu.md": {
	id: "yokohama/2026-05-27-taishin-kaishu.md";
  slug: "yokohama/2026-05-27-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokohama/2026-06-05-kizondannetsu-kaishu.md": {
	id: "yokohama/2026-06-05-kizondannetsu-kaishu.md";
  slug: "yokohama/2026-06-05-kizondannetsu-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokosuka/2026-05-08-juuten-taiyoko.md": {
	id: "yokosuka/2026-05-08-juuten-taiyoko.md";
  slug: "yokosuka/2026-05-08-juuten-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokosuka/2026-05-12-iju.md": {
	id: "yokosuka/2026-05-12-iju.md";
  slug: "yokosuka/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokosuka/2026-05-29-taishin-kaishu.md": {
	id: "yokosuka/2026-05-29-taishin-kaishu.md";
  slug: "yokosuka/2026-05-29-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokosuka/2026-06-05-shoene-kaishu.md": {
	id: "yokosuka/2026-06-05-shoene-kaishu.md";
  slug: "yokosuka/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokoze/2026-05-10-shoene.md": {
	id: "yokoze/2026-05-10-shoene.md";
  slug: "yokoze/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokoze/2026-05-10-taiyoko.md": {
	id: "yokoze/2026-05-10-taiyoko.md";
  slug: "yokoze/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yokoze/2026-06-03-taishin-kaishu.md": {
	id: "yokoze/2026-06-03-taishin-kaishu.md";
  slug: "yokoze/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yorii/2026-05-10-shoene.md": {
	id: "yorii/2026-05-10-shoene.md";
  slug: "yorii/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yorii/2026-05-10-taiyoko.md": {
	id: "yorii/2026-05-10-taiyoko.md";
  slug: "yorii/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yorii/2026-06-03-taishin-kaishu.md": {
	id: "yorii/2026-06-03-taishin-kaishu.md";
  slug: "yorii/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yosano/2026-05-11-taiyoko.md": {
	id: "yosano/2026-05-11-taiyoko.md";
  slug: "yosano/2026-05-11-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yosano/2026-06-03-taishin-kaishu.md": {
	id: "yosano/2026-06-03-taishin-kaishu.md";
  slug: "yosano/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yosano/2026-06-05-shoene-kaishu.md": {
	id: "yosano/2026-06-05-shoene-kaishu.md";
  slug: "yosano/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yoshikawa/2026-05-10-shoene.md": {
	id: "yoshikawa/2026-05-10-shoene.md";
  slug: "yoshikawa/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yoshikawa/2026-05-10-taiyoko.md": {
	id: "yoshikawa/2026-05-10-taiyoko.md";
  slug: "yoshikawa/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yoshikawa/2026-06-03-taishin-kaishu.md": {
	id: "yoshikawa/2026-06-03-taishin-kaishu.md";
  slug: "yoshikawa/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yoshimi/2026-05-10-shoene.md": {
	id: "yoshimi/2026-05-10-shoene.md";
  slug: "yoshimi/2026-05-10-shoene";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yoshimi/2026-05-10-taiyoko.md": {
	id: "yoshimi/2026-05-10-taiyoko.md";
  slug: "yoshimi/2026-05-10-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yoshimi/2026-06-03-taishin-kaishu.md": {
	id: "yoshimi/2026-06-03-taishin-kaishu.md";
  slug: "yoshimi/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yoshino_nara/2026-05-12-iju.md": {
	id: "yoshino_nara/2026-05-12-iju.md";
  slug: "yoshino_nara/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yoshino_nara/2026-05-12-taiyoko.md": {
	id: "yoshino_nara/2026-05-12-taiyoko.md";
  slug: "yoshino_nara/2026-05-12-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yoshino_nara/2026-06-03-taishin-kaishu.md": {
	id: "yoshino_nara/2026-06-03-taishin-kaishu.md";
  slug: "yoshino_nara/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yoshino_nara/2026-06-05-shoene-kaishu.md": {
	id: "yoshino_nara/2026-06-05-shoene-kaishu.md";
  slug: "yoshino_nara/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yugawara/2026-05-08-smart-energy.md": {
	id: "yugawara/2026-05-08-smart-energy.md";
  slug: "yugawara/2026-05-08-smart-energy";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yugawara/2026-05-12-iju.md": {
	id: "yugawara/2026-05-12-iju.md";
  slug: "yugawara/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yugawara/2026-06-03-taishin-kaishu.md": {
	id: "yugawara/2026-06-03-taishin-kaishu.md";
  slug: "yugawara/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"yugawara/2026-06-05-shoene-kaishu.md": {
	id: "yugawara/2026-06-05-shoene-kaishu.md";
  slug: "yugawara/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"zama/2026-05-08-smart-house.md": {
	id: "zama/2026-05-08-smart-house.md";
  slug: "zama/2026-05-08-smart-house";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"zama/2026-06-03-taishin-kaishu.md": {
	id: "zama/2026-06-03-taishin-kaishu.md";
  slug: "zama/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"zama/2026-06-05-shoene-kaishu.md": {
	id: "zama/2026-06-05-shoene-kaishu.md";
  slug: "zama/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"zushi/2026-05-08-juuten-taiyoko.md": {
	id: "zushi/2026-05-08-juuten-taiyoko.md";
  slug: "zushi/2026-05-08-juuten-taiyoko";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"zushi/2026-05-12-iju.md": {
	id: "zushi/2026-05-12-iju.md";
  slug: "zushi/2026-05-12-iju";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"zushi/2026-06-03-taishin-kaishu.md": {
	id: "zushi/2026-06-03-taishin-kaishu.md";
  slug: "zushi/2026-06-03-taishin-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
"zushi/2026-06-05-shoene-kaishu.md": {
	id: "zushi/2026-06-05-shoene-kaishu.md";
  slug: "zushi/2026-06-05-shoene-kaishu";
  body: string;
  collection: "subsidies";
  data: InferEntrySchema<"subsidies">
} & { render(): Render[".md"] };
};

	};

	type DataEntryMap = {
		
	};

	type AnyEntryMap = ContentEntryMap & DataEntryMap;

	export type ContentConfig = typeof import("./../../src/content/config.js");
}
