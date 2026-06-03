import os, json, glob, sys

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

raw_dir = r"C:\Users\noppy\Subsidy\data\raw\2026-05-16"
files = sorted(glob.glob(os.path.join(raw_dir, "kishiwada-*.json")))

out = open(r"C:\Users\noppy\Subsidy\kishiwada_content.txt", "w", encoding="utf-8")

for fp in files:
    with open(fp, encoding="utf-8") as f:
        data = json.load(f)
    out.write(f"\n{'='*60}\n")
    out.write(f"URL  : {data.get('url','')}\n")
    out.write(f"TITLE: {data.get('title','')}\n")
    out.write(f"TEXT :\n{(data.get('text','') or '')[:800]}\n")

out.close()
print("Done")
