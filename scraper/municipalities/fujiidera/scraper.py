import logging
from datetime import datetime, timezone
from typing import Iterator
from urllib.parse import urljoin, urlparse

from scraper.base_scraper import BaseScraper, RawSubsidy
from scraper.utils import http_client, html_parser, pdf_extractor

logger = logging.getLogger(__name__)


class FujiideraScraper(BaseScraper):
    def get_municipality_id(self) -> str:
        return "fujiidera"

    def discover_subsidy_urls(self) -> Iterator[str]:
        cfg = self.get_config()
        seen: set[str] = set()
        for index_url in cfg["index_urls"]:
            yield from self._crawl_index(index_url, cfg, seen)

    def _crawl_index(self, index_url: str, cfg: dict, seen: set[str]) -> Iterator[str]:
        rate = cfg["rate_limit_seconds"]
        selector = cfg["selectors"]["subsidy_link"]
        base_url = cfg["base_url"]
        try:
            resp = http_client.get(index_url, rate_limit=rate)
        except Exception as e:
            logger.warning("インデックスページ取得失敗 url=%s err=%s", index_url, e)
            return
        links = html_parser.extract_links(resp.content, index_url, selector)
        for link in links:
            if not link.startswith(base_url):
                continue
            parsed = urlparse(link)
            if parsed.path.lower().endswith(".pdf"):
                continue
            if link not in seen:
                seen.add(link)
                yield link

    def _is_subsidy_related(self, title: str, text: str) -> bool:
        keywords = self.get_config().get("subsidy_keywords", [])
        combined = title + text
        return any(kw in combined for kw in keywords)

    def fetch_raw_content(self, url: str) -> RawSubsidy | None:
        cfg = self.get_config()
        rate = cfg["rate_limit_seconds"]
        if self._is_pdf_url(url):
            return self._fetch_pdf(url, cfg, rate)
        return self._fetch_html(url, cfg, rate)

    def _fetch_html(self, url: str, cfg: dict, rate: float) -> RawSubsidy | None:
        try:
            resp = http_client.get(url, rate_limit=rate)
        except Exception as e:
            logger.warning("HTML取得失敗 url=%s err=%s", url, e)
            return None
        title = html_parser.extract_title(resp.content, cfg["selectors"]["title"])
        text = html_parser.clean_text(resp.content, cfg["selectors"]["body"])
        if not text.strip():
            logger.warning("本文なし url=%s", url)
            return None
        if not self._is_subsidy_related(title, text):
            logger.info("補助金関連キーワードなし スキップ url=%s", url)
            return None
        pdf_links = self._find_pdf_links(resp.content, url)
        for pdf_url in pdf_links[:3]:
            try:
                pdf_bytes = http_client.get_bytes(pdf_url, rate_limit=rate, max_bytes=cfg["pdf_max_bytes"])
                pdf_text = pdf_extractor.extract_text(pdf_bytes)
                if pdf_text.strip():
                    text += f"\n\n[PDFより]\n{pdf_text}"
            except ValueError as e:
                logger.warning("PDF スキップ url=%s err=%s", pdf_url, e)
            except Exception as e:
                logger.warning("PDF取得失敗 url=%s err=%s", pdf_url, e)
        return RawSubsidy(
            url=url, text=text, title=title,
            municipality_id=self.get_municipality_id(),
            source_type="html", scraped_at=datetime.now(timezone.utc),
        )

    def _fetch_pdf(self, url: str, cfg: dict, rate: float) -> RawSubsidy | None:
        try:
            pdf_bytes = http_client.get_bytes(url, rate_limit=rate, max_bytes=cfg["pdf_max_bytes"])
        except Exception as e:
            logger.warning("PDF取得失敗 url=%s err=%s", url, e)
            return None
        text = pdf_extractor.extract_text(pdf_bytes)
        if not text.strip():
            return None
        return RawSubsidy(
            url=url, text=text, title=urlparse(url).path.split("/")[-1],
            municipality_id=self.get_municipality_id(),
            source_type="pdf", scraped_at=datetime.now(timezone.utc),
        )

    @staticmethod
    def _is_pdf_url(url: str) -> bool:
        return urlparse(url).path.lower().endswith(".pdf")

    @staticmethod
    def _find_pdf_links(html: bytes, base_url: str) -> list[str]:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
        return [urljoin(base_url, a["href"]) for a in soup.find_all("a", href=True) if a["href"].lower().endswith(".pdf")]
