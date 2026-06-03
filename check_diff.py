import os, json, glob

seen_path = r"C:\Users\noppy\Subsidy\data\state\seen_urls.json"
raw_dir   = r"C:\Users\noppy\Subsidy\data\raw"

with open(seen_path, encoding="utf-8") as f:
    seen = json.load(f)

dates = sorted(os.listdir(raw_dir))
print(f"取得日一覧（最新5件）: {dates[-5:]}")

latest = dates[-1]
print(f"\n最新日: {latest}")

changed = []
new_urls = []
files = glob.glob(os.path.join(raw_dir, latest, "*.json"))
print(f"ファイル数: {len(files)}")

for fp in files:
    try:
        with open(fp, encoding="utf-8") as f:
            data = json.load(f)
        url = data.get("url", "")
        h   = data.get("content_hash", "")
        muni = data.get("municipality_id", "")
        old = seen.get(url)
        if old is None:
            new_urls.append((muni, url))
        elif old != h:
            changed.append({"muni": muni, "url": url, "old": old[:16], "new": h[:16]})
    except Exception:
        pass

print(f"\n新規URL: {len(new_urls)}件")
for muni, u in new_urls[:15]:
    print(f"  NEW [{muni}]: {u}")

print(f"\n変更あり: {len(changed)}件")
for c in changed[:15]:
    print(f"  CHANGED [{c['muni']}]: {c['url']}")
    print(f"    old hash: {c['old']}... → new: {c['new']}...")
