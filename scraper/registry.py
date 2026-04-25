import importlib
import logging
from pathlib import Path

from scraper.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


def get_all_scrapers() -> dict[str, BaseScraper]:
    """municipalities/ 配下を自動探索してスクレイパーを返す。"""
    scrapers: dict[str, BaseScraper] = {}
    municipalities_dir = Path(__file__).parent / "municipalities"

    for city_dir in sorted(municipalities_dir.iterdir()):
        if not city_dir.is_dir() or city_dir.name.startswith("_"):
            continue
        scraper_module_path = f"scraper.municipalities.{city_dir.name}.scraper"
        try:
            module = importlib.import_module(scraper_module_path)
        except ModuleNotFoundError:
            logger.warning("スクレイパーモジュールが見つかりません: %s", scraper_module_path)
            continue

        # BaseScraper を継承したクラスを探す
        scraper_class = None
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                isinstance(attr, type)
                and issubclass(attr, BaseScraper)
                and attr is not BaseScraper
            ):
                scraper_class = attr
                break

        if scraper_class is None:
            logger.warning("BaseScraper サブクラスが見つかりません: %s", scraper_module_path)
            continue

        instance = scraper_class()
        scrapers[instance.get_municipality_id()] = instance
        logger.info("スクレイパー登録: %s (%s)", instance.get_municipality_id(), instance.get_name())

    return scrapers
