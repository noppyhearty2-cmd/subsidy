#!/usr/bin/env python3
"""
Stage 1 (Jグランツ版): 全都道府県・主要市区町村の補助金をJグランツAPIで取得する。
タスクスケジューラから run_scraper.py と並行して実行する。

使い方:
  python scraper/run_jgrants.py                    # 全エリアを取得
  python scraper/run_jgrants.py -a 大阪府           # 指定エリアのみ
  python scraper/run_jgrants.py -a 大阪市 -k 太陽光  # キーワード絞り込み
  python scraper/run_jgrants.py --dry-run           # 保存せずカウントのみ
"""
import argparse
import json
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scraper.jgrants_client import search_by_area, DEFAULT_KEYWORDS  # noqa: E402

# ─── ディレクトリ設定 ─────────────────────────────────────
DATA_DIR  = ROOT / "data"
RAW_DIR   = DATA_DIR / "raw"
STATE_DIR = DATA_DIR / "state"
LOGS_DIR  = DATA_DIR / "run_logs"
SEEN_FILE = STATE_DIR / "seen_jgrants.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run_jgrants")

# ─── 取得対象エリア一覧 ────────────────────────────────────
# Jグランツ API の target_area_search は都道府県名のみ対応。
# 市区町村名（大阪市、横浜市 等）はすべて 400 Bad Request が返るため除外。
# 都道府県で検索すると、その県内の自治体補助金もまとめて取得できる。
SEARCH_AREAS = [
    # 北海道・東北
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    # 関東
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    # 中部
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県",
    # 近畿
    "三重県", "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    # 中国・四国
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    # 九州・沖縄
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]


def load_seen() -> dict:
    if SEEN_FILE.exists():
        with open(SEEN_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_seen(seen: dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump(seen, f, ensure_ascii=False, indent=2)


def write_log(log_path: Path, record: dict) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def run(
    area_filter: str | None = None,
    keyword: str = "",
    dry_run: bool = False,
) -> None:
    today    = date.today().isoformat()
    log_path = LOGS_DIR / f"{today}-jgrants.jsonl"
    out_dir  = RAW_DIR / today / "jgrants"

    areas = [area_filter] if area_filter else SEARCH_AREAS
    seen  = load_seen()

    total_saved = total_skipped = total_errors = 0

    write_log(log_path, {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": "run_start",
        "areas": areas,
        "dry_run": dry_run,
    })

    for area in areas:
        logger.info("=== %s 検索中 ===", area)
        area_saved = 0

        keywords = [keyword] if keyword else None
        try:
            for subsidy in search_by_area(area, keywords=keywords):
                sid = subsidy.subsidy_id
                chash = subsidy.content_hash

                # 変更なしならスキップ
                if seen.get(sid) == chash:
                    total_skipped += 1
                    continue

                if dry_run:
                    logger.info("[dry-run] 新規/更新: %s | %s | %s",
                                sid, subsidy.title[:40], subsidy.target_area)
                    total_saved += 1
                    continue

                # 保存
                out_dir.mkdir(parents=True, exist_ok=True)
                data = subsidy.to_raw_dict()
                # エリア名からmunicipality_idを推定（ベストエフォート）
                data["search_area"] = area
                out_path = out_dir / f"{sid}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)

                seen[sid] = chash
                area_saved += 1
                total_saved += 1
                logger.debug("保存: %s", out_path.name)

        except Exception as e:
            logger.error("エラー area=%s: %s", area, e)
            write_log(log_path, {
                "ts": datetime.now(timezone.utc).isoformat(),
                "event": "error",
                "area": area,
                "error": str(e),
            })
            total_errors += 1

        logger.info("%s: 保存=%d", area, area_saved)

    if not dry_run:
        save_seen(seen)

    summary = {
        "ts":      datetime.now(timezone.utc).isoformat(),
        "event":   "run_end",
        "saved":   total_saved,
        "skipped": total_skipped,
        "errors":  total_errors,
        "dry_run": dry_run,
    }
    write_log(log_path, summary)
    logger.info("完了: 保存=%d スキップ=%d エラー=%d",
                total_saved, total_skipped, total_errors)


# ─── CLI ─────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Jグランツ API から補助金データを取得して data/raw/ に保存する"
    )
    parser.add_argument(
        "-a", "--area",
        default=None,
        help="取得するエリア名（例: 大阪府, 大阪市）。省略時は全エリア。",
    )
    parser.add_argument(
        "-k", "--keyword",
        default="",
        help="キーワード絞り込み（例: 太陽光, 創業）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="保存せず件数のみ表示する",
    )
    args = parser.parse_args()
    run(area_filter=args.area, keyword=args.keyword, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
