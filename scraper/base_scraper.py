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


class BaseScraper(ABC):
    def __init__(self):
        self._config = self._load_config()

    def _load_config(self) -> dict:
        config_path = Path(__file__).parent / "municipalities" / self.get_municipality_id() / "config.yml"
        with open(config_path, encoding="utf-8") as f:
            return yaml.safe_load(f)

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
