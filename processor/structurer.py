import hashlib
import json
import logging
from datetime import datetime, timezone

from pydantic import ValidationError

from processor.schema import SubsidyRecord
from scraper.base_scraper import RawSubsidy

logger = logging.getLogger(__name__)


class Structurer:
    def __init__(self, claude_client):
        self._client = claude_client

    def process(self, raw: RawSubsidy) -> SubsidyRecord | None:
        """生テキストを Claude で構造化して SubsidyRecord を返す。失敗時は None。"""
        try:
            raw_json = self._client.structure_subsidy(raw.text, raw.url)
        except Exception as e:
            logger.error("Claude API エラー url=%s err=%s", raw.url, e)
            return None

        try:
            data = json.loads(self._extract_json(raw_json))
        except json.JSONDecodeError as e:
            logger.error("JSON解析失敗 url=%s err=%s raw=%s", raw.url, e, raw_json[:200])
            return None

        if data.get("error") == "not_a_subsidy":
            logger.info("補助金ページではないためスキップ url=%s", raw.url)
            return None

        data["municipality_id"] = raw.municipality_id
        data["source_url"] = raw.url
        data["scraped_at"] = datetime.now(timezone.utc)
        data["content_hash"] = hashlib.sha256(raw.text.encode()).hexdigest()
        data["is_active"] = _is_active(data.get("deadline"))

        try:
            return SubsidyRecord(**data)
        except ValidationError as e:
            logger.error("バリデーション失敗 url=%s err=%s", raw.url, e)
            return None

    @staticmethod
    def _extract_json(text: str) -> str:
        """Claude の返答から JSON 部分だけを抽出する。"""
        text = text.strip()
        # ```json ... ``` ブロックを除去
        if "```json" in text:
            start = text.index("```json") + 7
            end = text.index("```", start)
            return text[start:end].strip()
        if "```" in text:
            start = text.index("```") + 3
            end = text.index("```", start)
            return text[start:end].strip()
        # { } で囲まれた部分を抽出
        start = text.find("{")
        end = text.rfind("}") + 1
        if start != -1 and end > 0:
            return text[start:end]
        return text


def _is_active(deadline: str | None) -> bool:
    if deadline is None or deadline == "随時":
        return True
    try:
        from datetime import date
        import re
        if re.match(r"^\d{4}-\d{2}-\d{2}$", deadline):
            return date.fromisoformat(deadline) >= date.today()
    except Exception:
        pass
    return True
