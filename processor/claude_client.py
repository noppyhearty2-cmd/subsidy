import logging
import os
import time
from pathlib import Path

import anthropic
from anthropic import RateLimitError, APIStatusError

logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-6"
SYSTEM_PROMPT_PATH = Path(__file__).parent / "prompts" / "system_prompt.txt"


class ClaudeClient:
    def __init__(self):
        self._client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
        self._system_prompt = SYSTEM_PROMPT_PATH.read_text(encoding="utf-8")

    def structure_subsidy(self, raw_text: str, source_url: str) -> str:
        """
        補助金の生テキストを構造化 JSON + 記事本文に変換する。
        system prompt はプロンプトキャッシュを利用してコストを削減する。
        """
        user_content = (
            f"以下の補助金情報を指定の JSON 形式で出力してください。\n"
            f"source_url: {source_url}\n\n"
            f"---\n{raw_text[:8000]}\n---"
        )

        response = self._call_with_retry(user_content)
        self._log_usage(response.usage, source_url)
        return response.content[0].text

    def call_raw(self, user_message: str) -> str:
        """自己修復等で汎用的に Claude を呼び出す（system prompt キャッシュなし）。"""
        response = self._client.messages.create(
            model=MODEL,
            max_tokens=2048,
            messages=[{"role": "user", "content": user_message}],
        )
        return response.content[0].text

    def _call_with_retry(self, user_content: str, max_retries: int = 4):
        wait = 10
        for attempt in range(max_retries):
            try:
                return self._client.messages.create(
                    model=MODEL,
                    max_tokens=4096,
                    system=[
                        {
                            "type": "text",
                            "text": self._system_prompt,
                            "cache_control": {"type": "ephemeral"},
                        }
                    ],
                    messages=[{"role": "user", "content": user_content}],
                )
            except RateLimitError:
                if attempt == max_retries - 1:
                    raise
                logger.warning("レート制限。%d秒後にリトライ (attempt %d)", wait, attempt + 1)
                time.sleep(wait)
                wait = min(wait * 2, 300)
            except APIStatusError as e:
                if e.status_code >= 500 and attempt < max_retries - 1:
                    logger.warning("APIエラー %d。リトライします", e.status_code)
                    time.sleep(wait)
                    wait *= 2
                else:
                    raise

    def _log_usage(self, usage, source_url: str):
        logger.info(
            "Claude usage url=%s cache_creation=%s cache_read=%s output=%s",
            source_url,
            getattr(usage, "cache_creation_input_tokens", 0),
            getattr(usage, "cache_read_input_tokens", 0),
            usage.output_tokens,
        )
