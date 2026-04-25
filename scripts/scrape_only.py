"""
スクレイプのみ実行し、生テキストを data/raw/YYYY-MM-DD/ に保存する。
リモートエージェントが記事生成を行うための前処理スクリプト。

使い方:
  python scripts/scrape_only.py
  python scripts/scrape_only.py --municipality osaka
"""
import argparse
import hashlib
import json
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper import registry as scraper_registry
from storage.state_manager import StateManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("scrape_only")

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"


def run(municipality_filter: str | None = None) -> int:
    state = StateManager()
    scrapers = scraper_registry.get_all_scrapers()
    if municipality_filter:
        scrapers = {k: v for k, v in scrapers.items() if k == municipality_filter}

    today = date.today().isoformat()
    out_dir = RAW_DIR / today
    out_dir.mkdir(parents=True, exist_ok=True)

    total_new = 0

    for muni_id, scraper in scrapers.items():
        logger.info("スクレイプ開始: %s", muni_id)
        urls = list(scraper.discover_subsidy_urls())
        logger.info("URL 発見: %d 件", len(urls))

        for url in urls:
            content_hash = ""
            if state.is_done(url, content_hash):
                continue

            raw = scraper.fetch_raw_content(url)
            if raw is None:
                continue

            content_hash = hashlib.sha256(raw.text.encode()).hexdigest()
            if state.is_done(url, content_hash):
                continue

            slug = _make_slug(url)
            out_file = out_dir / f"{muni_id}-{slug}.json"
            data = {
                "url": raw.url,
                "title": raw.title,
                "municipality_id": raw.municipality_id,
                "source_type": raw.source_type,
                "text": raw.text,
                "content_hash": content_hash,
                "scraped_at": raw.scraped_at.isoformat(),
            }
            out_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            state.mark_processing(url, content_hash)
            total_new += 1
            logger.info("保存: %s", out_file.name)

    logger.info("新規 %d 件を %s に保存しました", total_new, out_dir)
    return total_new


def _make_slug(url: str) -> str:
    import re
    from urllib.parse import urlparse
    path = urlparse(url).path
    slug = re.sub(r"[^a-zA-Z0-9]", "-", path)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug[:40] or hashlib.sha256(url.encode()).hexdigest()[:12]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--municipality", default=None)
    args = parser.parse_args()
    count = run(municipality_filter=args.municipality)
    print(count)
    sys.exit(0)
