"""
パイプラインのエントリポイント。

使い方:
  python scripts/run_pipeline.py                          # 全自治体（スクレイプ＋記事生成）
  python scripts/run_pipeline.py --municipality osaka     # 大阪市のみ
  python scripts/run_pipeline.py --scrape-only            # スクレイプのみ（data/raw/ に保存、API不要）
  python scripts/run_pipeline.py --process-raw            # data/raw/ の未処理ファイルを記事生成
  python scripts/run_pipeline.py --output-count           # 新規件数を stdout に出力（GitHub Actions用）
"""
import argparse
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# プロジェクトルートを sys.path に追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper import registry as scraper_registry
from scraper.self_healer import attempt_heal
from storage.run_log import RunLog
from storage.state_manager import StateManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("pipeline")

RAW_DATA_DIR = Path(__file__).parent.parent / "data" / "raw"


def _has_api_key() -> bool:
    return bool(os.environ.get("ANTHROPIC_API_KEY", "").strip())


def run(municipality_filter: str | None = None, scrape_only: bool = False,
        process_raw: bool = False) -> int:
    """パイプラインを実行して新規件数を返す。"""
    # API キーが未設定の場合は自動的にスクレイプのみモードへ
    if not scrape_only and not process_raw and not _has_api_key():
        logger.warning("ANTHROPIC_API_KEY が未設定です。--scrape-only モードで実行します。")
        scrape_only = True

    if process_raw:
        return _process_raw_files(municipality_filter)

    # --- スクレイプフェーズ ---
    from storage.markdown_writer import MarkdownWriter
    state = StateManager()
    run_log = RunLog()

    if not scrape_only:
        from processor.claude_client import ClaudeClient
        from processor.structurer import Structurer
        claude = ClaudeClient()
        structurer = Structurer(claude)
        writer = MarkdownWriter()
    else:
        structurer = None
        writer = None

    scrapers = scraper_registry.get_all_scrapers()
    if municipality_filter:
        scrapers = {k: v for k, v in scrapers.items() if k == municipality_filter}
        if not scrapers:
            logger.error("指定された自治体が見つかりません: %s", municipality_filter)
            return 0

    total_new = 0

    for muni_id, scraper in scrapers.items():
        logger.info("=== %s (%s) 開始 ===", muni_id, scraper.get_name())
        counts = {"discovered": 0, "new": 0, "updated": 0, "skipped": 0, "failed": 0}

        urls = list(scraper.discover_subsidy_urls())
        counts["discovered"] = len(urls)

        if not urls:
            if scrape_only or not _has_api_key():
                logger.error("URL 0件。スキップします municipality=%s", muni_id)
                run_log.summary(muni_id, **counts)
                continue
            logger.warning("URL 0件。自己修復を試みます municipality=%s", muni_id)
            healed = attempt_heal(
                municipality_id=muni_id,
                failed_url=scraper.get_config()["index_urls"][0],
                error_detail="discover_subsidy_urls() returned 0 results",
                claude_client=claude,
            )
            run_log.auto_fix(muni_id, healed)
            if healed:
                scraper.__init__()
                urls = list(scraper.discover_subsidy_urls())
                counts["discovered"] = len(urls)
            if not urls:
                logger.error("修復後も URL 0件。スキップします municipality=%s", muni_id)
                run_log.summary(muni_id, **counts)
                continue

        for url in urls:
            if scrape_only:
                result = _scrape_and_save_raw(url, scraper, state, run_log)
            else:
                result = _process_url(url, scraper, structurer, writer, state, run_log, counts)
            if result == "new":
                counts["new"] += 1
                total_new += 1
            elif result == "updated":
                counts["updated"] += 1
                total_new += 1
            elif result == "skipped":
                counts["skipped"] += 1
            elif result == "failed":
                counts["failed"] += 1

        run_log.summary(muni_id, **counts)
        logger.info(
            "=== %s 完了: 新規%d件 更新%d件 スキップ%d件 失敗%d件 ===",
            muni_id, counts["new"], counts["updated"], counts["skipped"], counts["failed"]
        )

    return total_new


def _scrape_and_save_raw(url, scraper, state, run_log) -> str:
    """スクレイプして data/raw/YYYY-MM-DD/ に JSON 保存（API 不要）。"""
    if state.is_done(url, ""):
        return "skipped"

    state.mark_processing(url)

    raw = scraper.fetch_raw_content(url)
    if raw is None:
        state.mark_failed(url, reason="fetch_raw_content returned None")
        return "failed"

    content_hash = hashlib.sha256(raw.text.encode()).hexdigest()
    if state.is_done(url, content_hash):
        state.mark_skipped(url, content_hash, reason="content unchanged")
        return "skipped"

    # data/raw/YYYY-MM-DD/ に保存
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    raw_dir = RAW_DATA_DIR / today
    raw_dir.mkdir(parents=True, exist_ok=True)

    # ファイル名: "{municipality_id}-{url_path_slug}-{source_type}.json"
    from urllib.parse import urlparse
    url_path = urlparse(url).path.strip("/").replace("/", "-")
    filename = f"{raw.municipality_id}-{url_path}-{raw.source_type}.json"
    out_path = raw_dir / filename

    payload = {
        "url": raw.url,
        "title": raw.title,
        "municipality_id": raw.municipality_id,
        "source_type": raw.source_type,
        "text": raw.text,
        "content_hash": content_hash,
        "scraped_at": raw.scraped_at.isoformat(),
    }
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # state は "done" としてマークして再処理させる（process_raw で処理済みにする）
    state.mark_done(url, content_hash, str(out_path))
    run_log.append("raw_saved", url=url, municipality=raw.municipality_id,
                   output=str(out_path))
    logger.info("生データ保存: %s", out_path.name)
    return "new"


def _process_raw_files(municipality_filter: str | None = None) -> int:
    """data/raw/ 配下の未処理 JSON を記事生成する（API 必要）。"""
    if not _has_api_key():
        logger.error("ANTHROPIC_API_KEY が未設定のため --process-raw を実行できません。")
        return 0

    from processor.claude_client import ClaudeClient
    from processor.structurer import Structurer
    from scraper.base_scraper import RawSubsidy
    from storage.markdown_writer import MarkdownWriter

    claude = ClaudeClient()
    structurer = Structurer(claude)
    writer = MarkdownWriter()
    state = StateManager()
    run_log = RunLog()
    total_new = 0

    for json_path in sorted(RAW_DATA_DIR.rglob("*.json")):
        try:
            payload = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning("JSON 読み込みエラー %s: %s", json_path, e)
            continue

        url = payload["url"]
        content_hash = payload["content_hash"]
        muni_id = payload.get("municipality_id", "")

        if municipality_filter and muni_id != municipality_filter:
            continue

        # state で処理済みチェック（記事ファイルが存在する場合はスキップ）
        existing = state._state.get(state.make_key(url, content_hash), {})
        if existing.get("status") == "done" and existing.get("article_written"):
            logger.debug("処理済みスキップ: %s", url)
            continue

        raw = RawSubsidy(
            url=payload["url"],
            text=payload["text"],
            title=payload["title"],
            municipality_id=payload["municipality_id"],
            source_type=payload["source_type"],
            scraped_at=datetime.fromisoformat(payload["scraped_at"]),
        )

        record = structurer.process(raw)
        if record is None:
            logger.info("補助金ではないためスキップ: %s", url)
            continue

        output_path = writer.write(record)

        # 記事生成済みフラグをセット
        key = state.make_key(url, content_hash)
        entry = state._state.get(key, {})
        entry.update({
            "status": "done",
            "url": url,
            "content_hash": content_hash,
            "output_path": str(output_path),
            "article_written": True,
            "last_checked": datetime.now(timezone.utc).isoformat(),
        })
        state._state[key] = entry
        state._save()

        run_log.append("subsidy_written", url=url, municipality=muni_id,
                       title=record.title, output=str(output_path))
        logger.info("記事生成: %s", output_path.name)
        total_new += 1

    logger.info("process-raw 完了。新規記事: %d 件", total_new)
    return total_new


def _process_url(url, scraper, structurer, writer, state, run_log, counts) -> str:
    """1件のURLを処理。戻り値: "new" | "updated" | "skipped" | "failed"."""
    # まずコンテンツなしで処理済みチェック（URL単位）
    url_key = hashlib.sha256(url.encode()).hexdigest()
    if state.is_done(url, ""):
        return "skipped"

    state.mark_processing(url)

    raw = scraper.fetch_raw_content(url)
    if raw is None:
        state.mark_failed(url, reason="fetch_raw_content returned None")
        return "failed"

    # コンテンツハッシュで重複チェック（同じ内容なら再処理不要）
    content_hash = hashlib.sha256(raw.text.encode()).hexdigest()
    if state.is_done(url, content_hash):
        state.mark_skipped(url, content_hash, reason="content unchanged")
        return "skipped"

    record = structurer.process(raw)
    if record is None:
        state.mark_skipped(url, content_hash, reason="not_a_subsidy or structuring failed")
        return "skipped"

    output_path = writer.write(record)
    state.mark_done(url, content_hash, str(output_path))

    run_log.append(
        "subsidy_written",
        url=url,
        municipality=record.municipality_id,
        title=record.title,
        output=str(output_path),
    )
    return "new"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--municipality", default=None, help="自治体ID（省略時は全自治体）")
    parser.add_argument("--scrape-only", action="store_true",
                        help="スクレイプのみ実行し data/raw/ に保存（ANTHROPIC_API_KEY 不要）")
    parser.add_argument("--process-raw", action="store_true",
                        help="data/raw/ の未処理ファイルを記事生成（ANTHROPIC_API_KEY 必要）")
    parser.add_argument("--output-count", action="store_true",
                        help="新規件数のみ stdout に出力（GitHub Actions 用）")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.getLogger().setLevel(args.log_level)

    new_count = run(
        municipality_filter=args.municipality,
        scrape_only=args.scrape_only,
        process_raw=args.process_raw,
    )

    if args.output_count:
        print(new_count)
    else:
        logger.info("パイプライン完了。新規: %d 件", new_count)

    sys.exit(0)


if __name__ == "__main__":
    main()
