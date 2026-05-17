#!/usr/bin/env python3
"""
Jグランツ 詳細補完スクリプト

data/raw/{date}/jgrants/*.json に保存された一覧データに
詳細エンドポイントの情報（description, use_purpose, industry, portal_url 等）を追記する。

使い方:
  python scraper/enrich_jgrants.py                      # 最新日付のデータを補完
  python scraper/enrich_jgrants.py --date 2026-05-17    # 日付指定
  python scraper/enrich_jgrants.py --dry-run            # 変更せず内容を確認
"""
import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scraper.jgrants_client import get_detail  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("enrich_jgrants")

DATA_DIR = ROOT / "data"
RAW_DIR  = DATA_DIR / "raw"
RATE_SLEEP = 0.8  # 詳細API リクエスト間隔（秒）


def strip_html(html: str) -> str:
    """HTMLタグを除去してプレーンテキストを返す。"""
    text = re.sub(r"<[^>]+>", "", html)
    text = text.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def is_template_detail(detail_html: str | None) -> bool:
    """detail フィールドがプレースホルダーテンプレートか判定する。"""
    if not detail_html:
        return True
    return "ここに" in detail_html and "入力して下さい" in detail_html


def enrich_one(path: Path, dry_run: bool = False) -> dict:
    """1ファイルを詳細データで補完する。戻り値: {status, has_content}"""
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)

    # すでに補完済みならスキップ
    if raw.get("detail_fetched"):
        return {"status": "already_done", "has_content": bool(raw.get("description"))}

    subsidy_id = raw["subsidy_id"]
    detail_list = get_detail(subsidy_id)

    # get_detail はリスト or 辞書 or None を返す場合がある
    detail: dict | None = None
    if isinstance(detail_list, list) and detail_list:
        detail = detail_list[0]
    elif isinstance(detail_list, dict):
        detail = detail_list

    if detail is None:
        logger.warning("詳細取得失敗: %s", subsidy_id)
        return {"status": "error", "has_content": False}

    detail_html: str = detail.get("detail") or ""
    description = strip_html(detail_html) if not is_template_detail(detail_html) else ""

    enriched = {
        **raw,
        "detail_fetched": True,
        "description": description,
        "use_purpose": detail.get("use_purpose") or "",
        "industry": detail.get("industry") or "",
        "subsidy_rate": detail.get("subsidy_rate"),
        "portal_url": detail.get("front_subsidy_detail_page_url") or "",
        "institution_name": detail.get("institution_name") or raw.get("institution_name") or "",
    }

    has_content = bool(description)

    if dry_run:
        logger.info("[dry-run] %s has_content=%s description_len=%d",
                    subsidy_id, has_content, len(description))
        return {"status": "dry_run", "has_content": has_content}

    with open(path, "w", encoding="utf-8") as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)

    return {"status": "ok", "has_content": has_content}


def run(date_str: str | None = None, dry_run: bool = False) -> None:
    if date_str:
        jgrants_dir = RAW_DIR / date_str / "jgrants"
    else:
        # 最新の日付ディレクトリを探す
        dates = sorted([d for d in RAW_DIR.iterdir() if d.is_dir()], reverse=True)
        jgrants_dir = None
        for d in dates:
            candidate = d / "jgrants"
            if candidate.exists():
                jgrants_dir = candidate
                break

    if jgrants_dir is None or not jgrants_dir.exists():
        logger.error("jgrants ディレクトリが見つかりません: %s", jgrants_dir)
        return

    files = sorted(jgrants_dir.glob("*.json"))
    logger.info("補完対象: %d件 (%s)", len(files), jgrants_dir)

    total = ok = error = with_content = already = 0
    for path in files:
        total += 1
        result = enrich_one(path, dry_run=dry_run)
        status = result["status"]
        if status == "ok":
            ok += 1
            if result["has_content"]:
                with_content += 1
        elif status == "already_done":
            already += 1
            if result["has_content"]:
                with_content += 1
        elif status == "error":
            error += 1
        time.sleep(RATE_SLEEP)

    logger.info("完了: 合計=%d 補完=%d 既補完=%d エラー=%d うち説明あり=%d",
                total, ok, already, error, with_content)


def main() -> None:
    parser = argparse.ArgumentParser(description="Jグランツ詳細データを補完する")
    parser.add_argument("--date", default=None, help="日付（YYYY-MM-DD）。省略時は最新")
    parser.add_argument("--dry-run", action="store_true", help="保存せず確認のみ")
    args = parser.parse_args()
    run(date_str=args.date, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
