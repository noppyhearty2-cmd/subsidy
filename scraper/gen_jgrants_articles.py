#!/usr/bin/env python3
"""
Jグランツ補完済みJSONから Astro .md 記事を一括生成する。

使い方:
  python scraper/gen_jgrants_articles.py                      # 最新日付を処理
  python scraper/gen_jgrants_articles.py --date 2026-05-17
  python scraper/gen_jgrants_articles.py --dry-run            # ファイルを作らず確認
  python scraper/gen_jgrants_articles.py --pref 東京都        # 特定都道府県のみ
"""
import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DATA_DIR    = ROOT / "data" / "raw"
CONTENT_DIR = ROOT / "site" / "src" / "content" / "subsidies"

# ─── 都道府県名 → municipality_id マッピング ──────────────────
PREF_TO_MUNI: dict[str, str] = {
    "北海道": "hokkaido",
    "青森県": "aomori_pref",
    "岩手県": "iwate_pref",
    "宮城県": "miyagi_pref",
    "秋田県": "akita_pref",
    "山形県": "yamagata_pref",
    "福島県": "fukushima_pref",
    "茨城県": "ibaraki_pref",
    "栃木県": "tochigi_pref",
    "群馬県": "gunma_pref",
    "埼玉県": "saitama_pref",
    "千葉県": "chiba_pref",
    "東京都": "tokyo",
    "神奈川県": "kanagawa_pref",
    "新潟県": "niigata_pref",
    "富山県": "toyama_pref",
    "石川県": "ishikawa_pref",
    "福井県": "fukui_pref",
    "山梨県": "yamanashi_pref",
    "長野県": "nagano_pref",
    "岐阜県": "gifu_pref",
    "静岡県": "shizuoka_pref",
    "愛知県": "aichi_pref",
    "三重県": "mie_pref",
    "滋賀県": "shiga_pref",
    "京都府": "kyoto_pref",
    "大阪府": "osaka_pref",
    "兵庫県": "hyogo_pref",
    "奈良県": "nara_pref",
    "和歌山県": "wakayama_pref",
    "鳥取県": "tottori_pref",
    "島根県": "shimane_pref",
    "岡山県": "okayama_pref",
    "広島県": "hiroshima_pref",
    "山口県": "yamaguchi_pref",
    "徳島県": "tokushima_pref",
    "香川県": "kagawa_pref",
    "愛媛県": "ehime_pref",
    "高知県": "kochi_pref",
    "福岡県": "fukuoka_pref",
    "佐賀県": "saga_pref",
    "長崎県": "nagasaki_pref",
    "熊本県": "kumamoto_pref",
    "大分県": "oita_pref",
    "宮崎県": "miyazaki_pref",
    "鹿児島県": "kagoshima_pref",
    "沖縄県": "okinawa_pref",
}

# 都道府県 表示名
PREF_NAME: dict[str, str] = {v: k for k, v in PREF_TO_MUNI.items()}
# 既存のディレクトリ（修正なしで使えるもの）
EXISTING_MUNIS = {d.name for d in CONTENT_DIR.iterdir() if d.is_dir()}

# ─── タグ推定 ──────────────────────────────────────────────────
USE_PURPOSE_TAG = {
    "販路拡大・海外展開をしたい": ["販路拡大", "海外展示会", "中小企業"],
    "新たな事業を始めたい": ["創業", "中小企業"],
    "設備を導入したい": ["設備投資", "中小企業"],
    "人材の採用・育成をしたい": ["雇用", "人材育成", "中小企業"],
    "デジタル化を進めたい": ["DX", "デジタル化", "中小企業"],
    "省エネ・再エネに取り組みたい": ["省エネ", "太陽光", "環境"],
    "住まいを改善したい": ["住宅", "リフォーム"],
    "農林水産業を営みたい": ["農業"],
    "研究開発をしたい": ["研究開発", "中小企業"],
    "知的財産を守りたい": ["知的財産", "特許", "中小企業"],
    "障害者を雇用したい": ["障害者", "雇用", "中小企業"],
    "賃上げを行いたい": ["賃上げ", "雇用", "中小企業"],
    "外国人を雇用したい": ["雇用", "中小企業"],
    "移住したい": ["移住"],
    "観光振興をしたい": ["観光", "中小企業"],
}

INDUSTRY_TAG = {
    "製造業": ["製造業", "中小企業"],
    "農業、林業": ["農業"],
    "建設業": ["中小企業"],
    "情報通信業": ["DX", "中小企業"],
    "金融業、保険業": ["中小企業"],
    "医療、福祉": ["中小企業"],
    "宿泊業、飲食サービス業": ["観光", "中小企業"],
    "小売業": ["中小企業"],
}


def is_all_industries(ind: str) -> bool:
    """業種フィールドが「全業種」相当か判定する。"""
    return len(ind) > 100 or ind.count("/") > 5 or ind.count("、") > 5


def derive_tags(data: dict) -> list[str]:
    tags: list[str] = []
    use = data.get("use_purpose") or ""
    ind = data.get("industry") or ""
    title = data.get("title") or ""

    for key, t_list in USE_PURPOSE_TAG.items():
        if key in use:
            tags.extend(t_list)

    if not is_all_industries(ind):
        for key, t_list in INDUSTRY_TAG.items():
            if key in ind:
                tags.extend(t_list)

    # タイトルキーワード補強
    kw_map = {
        "雇用": "雇用", "人材": "人材育成", "研修": "研修", "スキル": "スキルアップ",
        "資格": "資格取得", "DX": "DX", "デジタル": "デジタル化", "AI": "AI活用",
        "特許": "特許", "商標": "知的財産", "省エネ": "省エネ", "太陽光": "太陽光",
        "融資": "融資", "奨学金": "奨学金", "移住": "移住", "観光": "観光",
        "農業": "農業", "障害": "障害者", "創業": "創業", "起業": "創業",
        "中小": "中小企業", "住宅": "住宅", "リフォーム": "リフォーム",
        "補助金": "中小企業", "助成金": "雇用",
    }
    for kw, tag in kw_map.items():
        if kw in title and tag not in tags:
            tags.append(tag)

    # 重複除去・上限
    seen: set = set()
    result: list[str] = []
    for t in tags:
        if t not in seen:
            seen.add(t)
            result.append(t)
    return result[:6] if result else ["中小企業"]


def fmt_amount(max_limit: str) -> str:
    n = int(max_limit or 0)
    if n == 0:
        return "要問合せ（Jグランツポータルで確認）"
    if n >= 1_000_000:
        return f"最大 **{n // 10_000}万円**"
    return f"最大 **{n:,}円**"


def fmt_deadline(dt_str: str) -> str | None:
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        # 2100年以降は「随時受付」扱い
        if dt.year > 2099:
            return None
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def slugify(title: str, subsidy_id: str) -> str:
    """記事スラグを生成。タイトルから数字・記号を除きASCII部分を使う。"""
    # サブシディIDの末尾8文字を使う
    short_id = subsidy_id[-8:].lower()
    return f"jgrants-{short_id}"


def extract_key_points(description: str, max_points: int = 4) -> list[str]:
    """説明文から箇条書きポイントを抽出する。"""
    if not description:
        return ["詳細はJグランツポータルでご確認ください"]

    points: list[str] = []
    # 補助率
    m = re.search(r"補助率[：:]\s*([^\n。]{3,30})", description)
    if m:
        points.append(f"補助率: {m.group(1).strip()}")

    # 対象者
    m = re.search(r"(?:対象者?|応募資格|補助対象者)[：:・]\s*([^\n。]{5,60})", description)
    if m:
        points.append(m.group(1).strip()[:50])

    # 上限
    m = re.search(r"(?:上限|最大)[^\d]*(\d[\d,万千百]+円)", description)
    if m:
        points.append(f"上限: {m.group(1)}")

    # 問合せ先
    m = re.search(r"問合せ先[：:\s]*([^\n]{5,40})", description)
    if m:
        points.append(f"問合せ: {m.group(1).strip()[:40]}")

    if not points:
        # descriptionの冒頭を短く切り出す
        first = description[:80].replace("\n", " ").strip()
        points.append(first + "…")

    return points[:max_points]


def generate_article(data: dict, muni_id: str, pref_name: str) -> str:
    title = data["title"]
    description = data.get("description") or ""
    max_limit = data.get("subsidy_max_limit") or "0"
    deadline_raw = data.get("acceptance_end_datetime") or ""
    deadline = fmt_deadline(deadline_raw)
    employees = data.get("target_number_of_employees") or ""
    use_purpose = data.get("use_purpose") or ""
    industry = data.get("industry") or ""
    subsidy_rate_raw = data.get("subsidy_rate") or ""
    portal_url = data.get("portal_url") or f"https://www.jgrants-portal.go.jp/subsidy/{data['subsidy_id']}"
    subsidy_id = data["subsidy_id"]

    tags = derive_tags(data)
    amount_str = fmt_amount(max_limit)
    key_points = extract_key_points(description)
    content_hash = f"{muni_id}-jgrants-{subsidy_id[-8:]}"

    # frontmatter の deadline
    deadline_fm = f"\ndeadline: \"{deadline}\"" if deadline else ""

    # amount の整形
    raw_amount_int = int(max_limit or 0)
    amount_display = f"最大{raw_amount_int // 10_000}万円" if raw_amount_int >= 10_000 else (f"最大{raw_amount_int:,}円" if raw_amount_int > 0 else "要問合せ")

    # 対象者（長すぎる業種リストを簡略化）
    target_parts = []
    if employees and employees != "従業員数の制約なし":
        target_parts.append(f"従業員{employees}の企業")
    if industry and not is_all_industries(industry):
        target_parts.append(industry)
    if use_purpose:
        target_parts.append(use_purpose)
    target_str = "・".join(target_parts) if target_parts else f"{pref_name}内の事業者・個人"

    # summary_ja
    desc_short = description[:60].replace("\n", " ") if description else ""
    summary_ja = f"{pref_name}のJグランツ掲載補助金。{desc_short}" if desc_short else f"{pref_name}の{title}。詳細はJグランツポータルでご確認ください。"
    summary_ja = summary_ja[:120]

    # タグのYAML形式
    tags_yaml = ", ".join(f'"{t}"' for t in tags)

    # key_pointsのYAML形式
    kp_yaml = "\n".join(f'  - "{kp}"' for kp in key_points)

    # 本文の説明（■見出しをMarkdown見出しに変換）
    if description:
        # 既知の見出しパターン（Jグランツの標準セクション）
        KNOWN_HEADINGS = [
            "目的・概要", "根拠法令", "応募資格", "地理条件", "備考",
            "問合せ先", "参照URL", "補助内容", "対象経費", "補助率",
            "補助対象者", "補助対象", "申請方法", "注意事項",
        ]
        heading_pattern = "|".join(re.escape(h) for h in KNOWN_HEADINGS)
        # ■見出し → \n### 見出し\n\n
        def replace_heading(m: re.Match) -> str:
            return f"\n\n### {m.group(1)}\n\n"
        desc_body = re.sub(
            r"■(" + heading_pattern + r")",
            replace_heading, description
        )
        # 残った ■ を改行付きで置換
        desc_body = desc_body.replace("■", "\n\n")
        desc_body = re.sub(r"\n{3,}", "\n\n", desc_body).strip()
    else:
        desc_body = f"詳細は[Jグランツポータル]({portal_url})をご確認ください。"

    # 補助率表示
    rate_line = f"\n- **補助率**: {subsidy_rate_raw}" if subsidy_rate_raw else ""

    # 募集期限
    deadline_body = f"**募集期限**: {deadline}" if deadline else "**募集期限**: 随時（詳細要確認）"

    # 従業員要件
    emp_line = f"\n- **従業員規模**: {employees}" if employees and employees != "従業員数の制約なし" else ""

    article = f"""---
title: "【2026年最新】{pref_name}｜{title}"
municipality: {muni_id}
target: "{target_str}"
amount: "{amount_display}"{deadline_fm}
tags: [{tags_yaml}]
key_points:
{kp_yaml}
source_url: "{portal_url}"
summary_ja: "{summary_ja}"
scraped_at: "2026-05-17T00:00:00Z"
is_active: true
content_hash: "{content_hash}"
---

## こんな人が対象

{target_str}が対象です。

{f"**業種**: {industry}" if industry and not is_all_industries(industry) else ""}
{f"**用途**: {use_purpose}" if use_purpose else ""}

## いくらもらえる？

{amount_str}{rate_line}{emp_line}

## 概要・詳細

{desc_body}

## 申し込み期限・方法

{deadline_body}

詳細・申請手続きは **Jグランツポータル** からご確認ください。

> 詳細・最新情報は必ず公式ページでご確認ください。
> Jグランツポータル: {portal_url}
> ※本記事はJグランツAPIの公開情報をもとに作成しています。内容が変更されている場合があります。
"""
    return article.strip()


def process_file(json_path: Path, dry_run: bool = False, overwrite: bool = False) -> dict:
    data = json.loads(json_path.read_text(encoding="utf-8"))

    if not data.get("detail_fetched"):
        return {"status": "not_enriched"}

    pref = data.get("search_area") or data.get("target_area") or ""
    muni_id = PREF_TO_MUNI.get(pref)
    if not muni_id:
        return {"status": "unknown_pref", "pref": pref}

    pref_name = PREF_NAME.get(muni_id, pref)
    slug = slugify(data["title"], data["subsidy_id"])
    out_dir = CONTENT_DIR / muni_id
    out_path = out_dir / f"2026-05-17-{slug}.md"

    # 既存記事チェック（overwrite=True で上書き）
    if out_path.exists() and not overwrite:
        return {"status": "exists", "path": str(out_path)}

    content = generate_article(data, muni_id, pref_name)

    if dry_run:
        print(f"\n{'='*60}")
        print(f"[{pref}→{muni_id}] {data['title'][:50]}")
        print(f"→ {out_path}")
        return {"status": "dry_run", "muni_id": muni_id}

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content, encoding="utf-8")
    return {"status": "created", "path": str(out_path), "muni_id": muni_id}


def run(date_str: str | None, pref_filter: str | None, dry_run: bool, overwrite: bool = False) -> None:
    if date_str:
        jgrants_dir = ROOT / "data" / "raw" / date_str / "jgrants"
    else:
        dates = sorted([d for d in DATA_DIR.iterdir() if d.is_dir()], reverse=True)
        jgrants_dir = None
        for d in dates:
            c = d / "jgrants"
            if c.exists():
                jgrants_dir = c
                break

    if not jgrants_dir or not jgrants_dir.exists():
        print("jgrants ディレクトリが見つかりません")
        return

    files = sorted(jgrants_dir.glob("*.json"))
    print(f"対象ファイル: {len(files)}件")

    counts: dict[str, int] = {}
    new_munis: set[str] = set()

    for path in files:
        data = json.loads(path.read_text(encoding="utf-8"))
        pref = data.get("search_area") or data.get("target_area") or ""
        if pref_filter and pref != pref_filter:
            continue

        result = process_file(path, dry_run=dry_run, overwrite=overwrite)
        status = result.get("status", "unknown")
        counts[status] = counts.get(status, 0) + 1

        muni_id = result.get("muni_id")
        if muni_id and status == "created" and muni_id not in EXISTING_MUNIS:
            new_munis.add(muni_id)

        if status == "created":
            print(f"✓ {pref} → {result['path'].split(chr(92))[-1]}")
        elif status == "exists":
            pass  # サイレント
        elif status not in ("dry_run",):
            print(f"  [{status}] {path.name}")

    print(f"\n--- 完了 ---")
    for s, c in sorted(counts.items()):
        print(f"  {s}: {c}")

    if new_munis:
        print(f"\n新規 municipality_id（index.astro への追加が必要）:")
        for m in sorted(new_munis):
            pref_n = PREF_NAME.get(m, m)
            print(f"  {m}: \"{pref_n}\"")


def main() -> None:
    parser = argparse.ArgumentParser(description="Jグランツ記事を一括生成する")
    parser.add_argument("--date", default=None)
    parser.add_argument("--pref", default=None, help="例: 東京都")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--overwrite", action="store_true", help="既存記事を上書きする")
    args = parser.parse_args()
    run(date_str=args.date, pref_filter=args.pref, dry_run=args.dry_run, overwrite=args.overwrite)


if __name__ == "__main__":
    main()
