import os, json, glob

raw_dir = r"C:\Users\noppy\Subsidy\data\raw\2026-05-16"
files = sorted(glob.glob(os.path.join(raw_dir, "*.json")))

# 全88件の自治体別集計 + テキスト冒頭確認
from collections import defaultdict
by_muni = defaultdict(list)

for fp in files:
    with open(fp, encoding="utf-8") as f:
        data = json.load(f)
    muni = data.get("municipality_id", "?")
    url  = data.get("url", "")
    title = data.get("title", "")
    text  = (data.get("text", "") or "")[:120].replace("\n", " ")
    by_muni[muni].append({"url": url, "title": title, "text": text})

for muni, items in sorted(by_muni.items()):
    print(f"\n=== {muni} ({len(items)}件) ===")
    for it in items[:5]:
        print(f"  [{it['title']}]")
        print(f"  {it['url']}")
        print(f"  {it['text'][:80]}")
