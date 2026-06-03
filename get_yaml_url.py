import sys, httpx, re, json
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

r = httpx.get("https://developers.digital.go.jp/documents/jgrants/api/", timeout=15, follow_redirects=True)
html = r.text

# keyword スキーマの前後を詳しく見る
idx = html.find('"keyword"')
if idx >= 0:
    print("=== keyword スキーマ周辺 ===")
    print(html[max(0, idx-200):idx+600])

# YAMLファイルのURLを探す（href や src に .yaml を含む）
print("\n=== yamlを含む全href・src ===")
for m in re.finditer(r'(href|src|url)=["\']([^"\']*)["\']', html):
    val = m.group(2)
    if "yaml" in val.lower() or "jgrants" in val.lower():
        print(f"  {m.group(1)}={val}")

# Next.js の __NEXT_DATA__ を探す
idx2 = html.find("__NEXT_DATA__")
if idx2 >= 0:
    # JSONを抽出
    start = html.find("{", idx2)
    # 簡易的にyamlを含む部分を探す
    snippet = html[idx2:idx2+5000]
    yaml_m = re.findall(r'"[^"]*yaml[^"]*"', snippet, re.IGNORECASE)
    print(f"\n=== __NEXT_DATA__ 内のyaml参照 ===")
    for m in yaml_m:
        print(f"  {m}")

# "jgrants-api" を含むURL
print("\n=== jgrants-api を含むURL ===")
for m in re.finditer(r'"(https?://[^"]*jgrants[^"]*)"', html):
    print(f"  {m.group(1)}")

# microcms 等の外部コンテンツURL
print("\n=== 画像・ファイルURL ===")
for m in re.finditer(r'"(https://[^"]*\.(yaml|pdf|json|zip))"', html):
    print(f"  {m.group(1)}")
