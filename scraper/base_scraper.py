from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator

import yaml


@dataclass
class RawSubsidy:
    url: str
    text: str
    title: str
    municipality_id: str
    source_type: str  # "html" or "pdf"
    scraped_at: datetime


# ─── デフォルト設定 ────────────────────────────────────────────
# 新規自治体の config.yml で省略した項目はこの値が使われる。
# 既存の config.yml に値が書かれていればそちらが優先される。

DEFAULT_SELECTORS: dict[str, str] = {
    "title": "h1",
    # カンマ区切りで先頭から順に試す（BeautifulSoup の select_one が対応）
    "body": "main, #main, #container, #HONBUN, #skip, article, .main-col",
    "subsidy_link": "a[href]",
}

DEFAULT_KEYWORDS: list[str] = [
    "助成", "補助", "給付", "手当", "無償", "支援金", "奨励金", "融資",
    "創業", "中小企業", "リフォーム", "省エネ", "耐震", "免除",
    "割引", "軽減", "DX", "デジタル", "太陽光", "ZEH",
    "雇用", "奨学金", "空き家", "出店", "商店街", "利子補給",
    "移住", "子育て", "医療費", "障害",
]

DEFAULT_CONFIG: dict = {
    "rate_limit_seconds": 1.5,
    "pdf_max_bytes": 10_485_760,
    "selectors": DEFAULT_SELECTORS,
    "subsidy_keywords": DEFAULT_KEYWORDS,
}


class BaseScraper(ABC):
    def __init__(self):
        self._config = self._load_config()

    def _load_config(self) -> dict:
        config_path = Path(__file__).parent / "municipalities" / self.get_municipality_id() / "config.yml"
        with open(config_path, encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        # デフォルト値とマージ（config.yml の値が優先）
        merged = {**DEFAULT_CONFIG, **raw}
        merged["selectors"] = {**DEFAULT_SELECTORS, **raw.get("selectors", {})}
        if "subsidy_keywords" not in raw:
            merged["subsidy_keywords"] = DEFAULT_KEYWORDS
        return merged

    @abstractmethod
    def get_municipality_id(self) -> str:
        ...

    @abstractmethod
    def discover_subsidy_urls(self) -> Iterator[str]:
        """自治体の補助金一覧ページから個別URL一覧を返す"""
        ...

    @abstractmethod
    def fetch_raw_content(self, url: str) -> RawSubsidy | None:
        """1件の補助金ページからテキストを抽出して返す"""
        ...

    def get_config(self) -> dict:
        return self._config

    def get_name(self) -> str:
        return self._config.get("name", self.get_municipality_id())
