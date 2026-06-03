import os, json, glob

raw_dir = r"C:\Users\noppy\Subsidy\data\raw\2026-05-16"
files = sorted(glob.glob(os.path.join(raw_dir, "kishiwada-*.json")))

for fp in files:
    with open(fp, encoding="utf-8") as f:
        data = json.load(f)
    print(f"\n{'='*60}")
    print(f"URL  : {data.get('url','')}")
    print(f"TITLE: {data.get('title','')}")
    print(f"TEXT :\n{(data.get('text','') or '')[:600]}")
