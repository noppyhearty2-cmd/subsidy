"""Jグランツ API の生レスポンス確認"""
import sys, httpx, json
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE = "https://api.jgrants-portal.go.jp/exp/v1/public"

# ① エリア指定なし（全国）
print("=== ① 全国・補助金 ===")
r = httpx.get(f"{BASE}/subsidies", params={"keyword": "補助金", "acceptance": "1"}, timeout=30)
print(f"  status: {r.status_code}")
if r.status_code == 200:
    data = r.json()
    print(f"  keys: {list(data.keys())}")
    items = data.get("result", []) or []
    print(f"  件数: {len(items)}")
    if items:
        print(f"  1件目: {items[0].get('title', '')}")
        print(f"  エリアkey: {[k for k in items[0].keys() if 'area' in k.lower()]}")
else:
    print(f"  body: {r.text[:300]}")

# ② target_area_search を試す
print("\n=== ② target_area_search=大阪府 ===")
r2 = httpx.get(f"{BASE}/subsidies", params={
    "keyword": "補助金",
    "acceptance": "1",
    "target_area_search": "大阪府"
}, timeout=30)
print(f"  status: {r2.status_code}")
print(f"  body: {r2.text[:400]}")

# ③ 別のエリアパラメータ名を試す
print("\n=== ③ keyword のみ（都道府県コード方式か確認） ===")
r3 = httpx.get(f"{BASE}/subsidies", params={
    "keyword": "大阪",
    "acceptance": "1",
}, timeout=30)
print(f"  status: {r3.status_code}")
if r3.status_code == 200:
    data3 = r3.json()
    items3 = data3.get("result", []) or []
    print(f"  件数: {len(items3)}")
    if items3:
        print(f"  1件目: {items3[0].get('title','')}")
        print(f"  全キー: {list(items3[0].keys())[:15]}")

# ④ v2 API を試す
print("\n=== ④ v2 API 確認 ===")
r4 = httpx.get("https://api.jgrants-portal.go.jp/exp/v2/public/subsidies",
               params={"keyword": "補助金", "acceptance": "1"}, timeout=30)
print(f"  status: {r4.status_code}")
print(f"  body[:200]: {r4.text[:200]}")
