#!/usr/bin/env python3
"""
Stage 1: スクレイプエントリポイント
タスクスケジューラから毎日23時に実行される。
各自治体のページを巡回し、変更があれば data/raw/ に JSON で保存する。
"""
import argparse
import hashlib
import json
import logging
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

# プロジェクトルートをsys.pathに追加（どこから実行しても動くように）
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scraper.registry import get_all_scrapers  # noqa: E402

# ─── ディレクトリ設定 ───────────────────────────────────────
DATA_DIR  = ROOT / "data"
RAW_DIR   = DATA_DIR / "raw"
STATE_DIR = DATA_DIR / "state"
LOGS_DIR  = DATA_DIR / "run_logs"
SEEN_FILE = STATE_DIR / "seen_urls.json"

# ─── ログ設定 ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run_scraper")


# ─── seen_urls.json の読み書き ────────────────────────────

def load_seen() -> dict:
    """処理済みハッシュの辞書を返す。"""
    if SEEN_FILE.exists():
        with open(SEEN_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_seen(seen: dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump(seen, f, ensure_ascii=False, indent=2)


# ─── ユーティリティ ──────────────────────────────────────

def compute_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def url_to_slug(url: str) -> str:
    """URLからファイル名に使えるスラグを生成する。"""
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    slug = re.sub(r"[^a-zA-Z0-9_]", "-", path)
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    return slug[:80]


def save_raw(raw_subsidy, content_hash: str) -> Path:
    """RawSubsidyをdata/raw/YYYY-MM-DD/に保存してパスを返す。"""
    today = date.today().isoformat()
    out_dir = RAW_DIR / today
    out_dir.mkdir(parents=True, exist_ok=True)

    slug = url_to_slug(raw_subsidy.url)
    filename = f"{raw_subsidy.municipality_id}-{slug}-{raw_subsidy.source_type}.json"
    out_path = out_dir / filename

    data = {
        "url":             raw_subsidy.url,
        "title":           raw_subsidy.title,
        "municipality_id": raw_subsidy.municipality_id,
        "source_type":     raw_subsidy.source_type,
        "text":            raw_subsidy.text,
        "content_hash":    content_hash,
        "scraped_at":      raw_subsidy.scraped_at.isoformat(),
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return out_path


def write_log(log_path: Path, record: dict) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


# ─── メイン処理 ──────────────────────────────────────────

def run(municipality_filter: str | None = None) -> None:
    today     = date.today().isoformat()
    log_path  = LOGS_DIR / f"{today}.jsonl"

    # スクレイパー一覧を取得
    scrapers = get_all_scrapers()
    if municipality_filter:
        if municipality_filter not in scrapers:
            logger.error("自治体が見つかりません: %s", municipality_filter)
            logger.info("利用可能: %s", sorted(scrapers.keys()))
            sys.exit(1)
        scrapers = {municipality_filter: scrapers[municipality_filter]}

    seen = load_seen()
    total_saved = total_skipped = total_errors = 0

    write_log(log_path, {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": "run_start",
        "municipalities": list(scrapers.keys()),
    })

    for muni_id, scraper in scrapers.items():
        logger.info("=== %s 開始 ===", muni_id)
        muni_saved = 0

        # ── URL 探索 ────────────────────────────────────
        try:
            urls = list(scraper.discover_subsidy_urls())
        except Exception as e:
            logger.error("URL探索失敗 muni=%s err=%s", muni_id, e)
            write_log(log_path, {
                "ts":           datetime.now(timezone.utc).isoformat(),
                "event":        "discover_error",
                "municipality": muni_id,
                "error":        str(e),
            })
            total_errors += 1
            continue

        logger.info("%s: %d件のURLを発見", muni_id, len(urls))

        # ── 各URLをフェッチ ──────────────────────────────
        for url in urls:
            try:
                raw = scraper.fetch_raw_content(url)
            except Exception as e:
                logger.warning("fetch失敗 url=%s err=%s", url, e)
                write_log(log_path, {
                    "ts":           datetime.now(timezone.utc).isoformat(),
                    "event":        "fetch_error",
                    "url":          url,
                    "municipality": muni_id,
                    "error":        str(e),
                })
                total_errors += 1
                continue

            # キーワード不一致 / 本文なし → None が返る
            if raw is None:
                total_skipped += 1
                continue

            content_hash = compute_hash(raw.text)

            # 処理済み（status='done'）ならスキップ
            if content_hash in seen and seen[content_hash].get("status") == "done":
                logger.debug("スキップ（処理済み） url=%s", url)
                total_skipped += 1
                continue

            # 新規 or 変更あり → 保存
            out_path = save_raw(raw, content_hash)
            seen[content_hash] = {
                "status":       "pending",
                "url":          url,
                "first_seen":   seen.get(content_hash, {}).get(
                                    "first_seen",
                                    datetime.now(timezone.utc).isoformat()),
                "processed_at": None,
            }

            write_log(log_path, {
                "ts":           datetime.now(timezone.utc).isoformat(),
                "event":        "raw_saved",
                "url":          url,
                "municipality": muni_id,
                "output":       str(out_path),
            })
            logger.info("保存: %s → %s", url, out_path.name)
            total_saved  += 1
            muni_saved   += 1

        logger.info("%s: 保存 %d件", muni_id, muni_saved)

    # ── 後処理 ──────────────────────────────────────────
    save_seen(seen)

    summary = {
        "ts":      datetime.now(timezone.utc).isoformat(),
        "event":   "run_complete",
        "saved":   total_saved,
        "skipped": total_skipped,
        "errors":  total_errors,
    }
    write_log(log_path, summary)
    logger.info("完了: 保存=%d スキップ=%d エラー=%d", total_saved, total_skipped, total_errors)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="補助金スクレイパー Stage 1: 各自治体ページを巡回してdata/raw/に保存する"
    )
    parser.add_argument(
        "--municipality", "-m",
        help="特定の自治体のみ実行（例: osaka, suita）省略時は全自治体"
    )
    args = parser.parse_args()
    run(municipality_filter=args.municipality)
