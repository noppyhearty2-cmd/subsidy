"""
パイプラインのエントリポイント。

使い方:
  python scripts/run_pipeline.py                        # 全自治体
  python scripts/run_pipeline.py --municipality osaka   # 大阪市のみ
  python scripts/run_pipeline.py --output-count         # 新規件数を stdout に出力（GitHub Actions用）
"""
import argparse
import hashlib
import logging
import sys
from pathlib import Path

# プロジェクトルートを sys.path に追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from processor.claude_client import ClaudeClient
from processor.structurer import Structurer
from scraper import registry as scraper_registry
from scraper.self_healer import attempt_heal
from storage.markdown_writer import MarkdownWriter
from storage.run_log import RunLog
from storage.state_manager import StateManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("pipeline")


def run(municipality_filter: str | None = None) -> int:
    """パイプラインを実行して新規件数を返す。"""
    claude = ClaudeClient()
    structurer = Structurer(claude)
    writer = MarkdownWriter()
    state = StateManager()
    run_log = RunLog()

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
            logger.warning("URL 0件。自己修復を試みます municipality=%s", muni_id)
            healed = attempt_heal(
                municipality_id=muni_id,
                failed_url=scraper.get_config()["index_urls"][0],
                error_detail="discover_subsidy_urls() returned 0 results",
                claude_client=claude,
            )
            run_log.auto_fix(muni_id, healed)
            if healed:
                # scraper を再インスタンス化して再試行
                scraper.__init__()
                urls = list(scraper.discover_subsidy_urls())
                counts["discovered"] = len(urls)
            if not urls:
                logger.error("修復後も URL 0件。スキップします municipality=%s", muni_id)
                run_log.summary(muni_id, **counts)
                continue

        for url in urls:
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
    parser.add_argument("--output-count", action="store_true",
                        help="新規件数のみ stdout に出力（GitHub Actions 用）")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.getLogger().setLevel(args.log_level)

    new_count = run(municipality_filter=args.municipality)

    if args.output_count:
        print(new_count)
    else:
        logger.info("パイプライン完了。新規: %d 件", new_count)

    sys.exit(0)


if __name__ == "__main__":
    main()
