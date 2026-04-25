import json
import logging
from datetime import date
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

PATCHES_DIR = Path(__file__).parent.parent / "patches"
PATCHES_DIR.mkdir(exist_ok=True)


def attempt_heal(municipality_id: str, failed_url: str, error_detail: str,
                 claude_client) -> bool:
    """
    スクレイプ失敗時に config.yml のセレクタを自動修復を試みる。
    成功すれば config.yml を上書きして True を返す。
    失敗すればパッチファイルを保存して False を返す。
    """
    config_path = (
        Path(__file__).parent / "municipalities" / municipality_id / "config.yml"
    )
    if not config_path.exists():
        logger.error("config.yml が見つかりません: %s", config_path)
        return False

    with open(config_path, encoding="utf-8") as f:
        current_config_text = f.read()

    # 失敗ページの HTML を取得
    html_snippet = _fetch_html_snippet(failed_url)
    if not html_snippet:
        logger.warning("自己修復: HTML取得失敗 url=%s", failed_url)
        return False

    # Claude に分析させる
    suggestion = _ask_claude_for_selectors(
        html_snippet=html_snippet,
        current_config=current_config_text,
        error_detail=error_detail,
        municipality_id=municipality_id,
        claude_client=claude_client,
    )
    if not suggestion:
        _save_patch(municipality_id, current_config_text, None, error_detail)
        return False

    # 修正を一時適用してスクレイプを再試行
    new_config = _apply_suggestion(current_config_text, suggestion)
    if _verify_config(new_config, municipality_id, failed_url):
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(new_config)
        logger.info("自己修復成功: config.yml を更新しました municipality=%s", municipality_id)
        return True

    _save_patch(municipality_id, current_config_text, new_config, error_detail)
    logger.warning("自己修復失敗: パッチを保存しました municipality=%s", municipality_id)
    return False


def _fetch_html_snippet(url: str, max_bytes: int = 50_000) -> str:
    try:
        from scraper.utils.http_client import get
        resp = get(url, rate_limit=1.0)
        html = resp.text[:max_bytes]
        return html
    except Exception as e:
        logger.warning("HTML取得失敗: %s", e)
        return ""


def _ask_claude_for_selectors(
    html_snippet: str,
    current_config: str,
    error_detail: str,
    municipality_id: str,
    claude_client,
) -> dict | None:
    prompt_path = Path(__file__).parent.parent / "processor" / "prompts" / "healer_prompt.txt"
    template = prompt_path.read_text(encoding="utf-8")

    user_message = template.format(
        municipality_id=municipality_id,
        error_detail=error_detail,
        current_config=current_config,
        html_snippet=html_snippet,
    )

    try:
        raw = claude_client.call_raw(user_message)
        data = json.loads(raw)
        if "selectors" in data:
            return data
    except Exception as e:
        logger.warning("Claude セレクタ提案解析失敗: %s", e)
    return None


def _apply_suggestion(current_config_text: str, suggestion: dict) -> str:
    config = yaml.safe_load(current_config_text)
    if "selectors" in suggestion:
        config["selectors"].update(suggestion["selectors"])
    return yaml.dump(config, allow_unicode=True, default_flow_style=False)


def _verify_config(new_config_yaml: str, municipality_id: str, test_url: str) -> bool:
    """新しい config で実際にスクレイプが成功するか検証する。"""
    try:
        new_config = yaml.safe_load(new_config_yaml)
        from scraper.utils.http_client import get
        from scraper.utils.html_parser import extract_links
        resp = get(test_url, rate_limit=1.0)
        links = extract_links(
            resp.content,
            new_config.get("base_url", ""),
            new_config["selectors"]["subsidy_link"],
        )
        return len(links) > 0
    except Exception as e:
        logger.warning("修復検証失敗: %s", e)
        return False


def _save_patch(municipality_id: str, original: str, suggested: str | None, error: str):
    today = date.today().isoformat()
    patch_path = PATCHES_DIR / f"{today}-{municipality_id}.yml.patch"
    content = {
        "municipality_id": municipality_id,
        "date": today,
        "error": error,
        "original_config": original,
        "suggested_config": suggested,
        "status": "needs_manual_review",
    }
    with open(patch_path, "w", encoding="utf-8") as f:
        yaml.dump(content, f, allow_unicode=True, default_flow_style=False)
    logger.info("パッチ保存: %s", patch_path)
