"""
Jグランツ API の現状確認スクリプト
サイトに掲載している主要自治体・都道府県で試し検索し、取得件数を確認する。
"""
import sys, time, json
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, r"C:\Users\noppy\Subsidy")

from scraper.jgrants_client import search, search_by_area

# ── テスト対象エリア ─────────────────────────────────────────
# 都道府県レベル（広域）と市区町村レベル（ピンポイント）を混在させて確認
AREAS = [
    # 関西
    ("大阪府",   "pref"),
    ("大阪市",   "city"),
    ("堺市",     "city"),
    ("兵庫県",   "pref"),
    ("神戸市",   "city"),
    ("京都府",   "pref"),
    ("滋賀県",   "pref"),
    ("奈良県",   "pref"),
    ("和歌山県", "pref"),
    # 関東
    ("神奈川県", "pref"),
    ("横浜市",   "city"),
    ("埼玉県",   "pref"),
    ("さいたま市","city"),
    ("東京都",   "pref"),
    # 東海
    ("愛知県",   "pref"),
    ("名古屋市", "city"),
]

KEYWORDS = ["補助金", "助成金", "支援金"]

print(f"{'エリア':<14} {'種別':<5} ", end="")
for kw in KEYWORDS:
    print(f" [{kw}]", end="")
print("  合計(重複除去)")
print("-" * 70)

total_all = 0
for area, kind in AREAS:
    counts = []
    ids_seen = set()
    for kw in KEYWORDS:
        result = search(kw, area_name=area, acceptance="1")
        counts.append(len(result))
        ids_seen.update(r.subsidy_id for r in result)
        time.sleep(0.5)
    total = len(ids_seen)
    total_all += total
    row = f"{area:<14} {kind:<5} "
    row += "".join(f"  {c:>4}" for c in counts)
    row += f"   {total:>4}件"
    print(row)

print("-" * 70)
print(f"{'合計':14}       {'':>20}   {total_all:>4}件")

# 大阪府で最初の5件の内容を確認
print("\n--- 大阪府 補助金 上位5件サンプル ---")
sample = search("補助金", area_name="大阪府", acceptance="1")
for s in sample[:5]:
    print(f"  [{s.subsidy_id}] {s.title}")
    print(f"    機関: {s.institution_name} / エリア: {s.target_area}")
    print(f"    上限: {s.subsidy_max_limit} / 期限: {s.acceptance_end_datetime}")
