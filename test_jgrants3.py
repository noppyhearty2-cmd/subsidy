"""Jグランツ API エンドポイント調査"""
import sys, httpx
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE = "https://api.jgrants-portal.go.jp/exp/v1/public"

# acceptance を外してみる
print("=== acceptance なし ===")
r = httpx.get(f"{BASE}/subsidies", params={"keyword": "補助金"}, timeout=30)
print(f"status: {r.status_code}, body: {r.text[:300]}")

# keyword を英語にする
print("\n=== keyword=subsidy ===")
r2 = httpx.get(f"{BASE}/subsidies", params={"keyword": "subsidy"}, timeout=30)
print(f"status: {r2.status_code}, body: {r2.text[:300]}")

# パラメータなし
print("\n=== パラメータなし ===")
r3 = httpx.get(f"{BASE}/subsidies", timeout=30)
print(f"status: {r3.status_code}, body: {r3.text[:300]}")

# 別のベースURL
print("\n=== ベースURL確認 ===")
r4 = httpx.get("https://api.jgrants-portal.go.jp/", timeout=30)
print(f"status: {r4.status_code}, body: {r4.text[:300]}")

# openapi spec を確認
print("\n=== openapi.json 確認 ===")
r5 = httpx.get("https://api.jgrants-portal.go.jp/exp/v1/public/openapi.json", timeout=30)
print(f"status: {r5.status_code}")
if r5.status_code == 200:
    import json
    spec = r5.json()
    paths = list(spec.get("paths", {}).keys())
    print(f"paths: {paths[:10]}")
else:
    print(f"body: {r5.text[:200]}")

# swagger
print("\n=== Swagger ===")
r6 = httpx.get("https://api.jgrants-portal.go.jp/exp/v1/public/subsidies",
               params={"keyword": "補助", "acceptance": "0"}, timeout=30)
print(f"status: {r6.status_code}, body: {r6.text[:300]}")
