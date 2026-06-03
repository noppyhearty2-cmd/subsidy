"""
Jグランツ API クライアント
https://api.jgrants-portal.go.jp/exp/v1/public/subsidies

認証不要・無料の公開REST APIを使って補助金情報を取得する。

API仕様メモ:
  - keyword は必須（最低2文字）
  - acceptance: "0"=全て, "1"=募集中のみ
  - target_area_search: 都道府県名・市区町村名で絞り込み（省略可）
  - ページネーションなし（全件一括返却）
"""
import hashlib
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterator

import httpx

logger = logging.getLogger(__name__)

BASE_URL   = "https://api.jgrants-portal.go.jp/exp/v1/public"
RATE_SLEEP = 0.5  # リクエスト間隔（秒）

# 検索キーワード群（複数キーワードで幅広く取得）
DEFAULT_KEYWORDS = ["補助金", "助成金", "支援金"]


@dataclass
class JGrantsSubsidy:
    """Jグランツから取得した補助金の生データ。"""
    subsidy_id: str
    title: str
    target_area: str
    subsidy_max_limit: str
    acceptance_start_datetime: str
    acceptance_end_datetime: str
    target_number_of_employees: str
    institution_name: str
    raw: dict = field(repr=False)

    @property
    def content_hash(self) -> str:
        key = f"{self.subsidy_id}:{self.title}:{self.acceptance_end_datetime}"
        return hashlib.sha256(key.encode()).hexdigest()

    @property
    def scraped_at(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def to_raw_dict(self) -> dict:
        """data/raw/ に保存するJSON形式に変換。"""
        return {
            "source": "jgrants",
            "subsidy_id": self.subsidy_id,
            "municipality_id": None,   # 呼び出し元で設定
            "title": self.title,
            "target_area": self.target_area,
            "subsidy_max_limit": self.subsidy_max_limit,
            "acceptance_start_datetime": self.acceptance_start_datetime,
            "acceptance_end_datetime": self.acceptance_end_datetime,
            "target_number_of_employees": self.target_number_of_employees,
            "institution_name": self.institution_name,
            "content_hash": self.content_hash,
            "scraped_at": self.scraped_at,
            "raw": self.raw,
        }


def _parse_subsidy(item: dict) -> JGrantsSubsidy:
    return JGrantsSubsidy(
        subsidy_id=str(item.get("id", "")),
        title=item.get("title", ""),
        target_area=item.get("target_area_search", ""),
        subsidy_max_limit=str(item.get("subsidy_max_limit", "")),
        acceptance_start_datetime=item.get("acceptance_start_datetime", ""),
        acceptance_end_datetime=item.get("acceptance_end_datetime", ""),
        target_number_of_employees=str(item.get("target_number_of_employees", "")),
        institution_name=item.get("institution_name", ""),
        raw=item,
    )


def search(
    keyword: str,
    area_name: str = "",
    acceptance: str = "1",
) -> list[JGrantsSubsidy]:
    """
    Jグランツ補助金を検索して一覧を返す。

    Args:
        keyword: 検索キーワード（必須・最低2文字）
        area_name: エリア絞り込み（例: "大阪府", "大阪市"）。省略時は全国。
        acceptance: "0"=全て, "1"=募集中のみ（デフォルト）
    """
    params: dict = {
        "keyword": keyword,
        "acceptance": acceptance,
        "sort": "acceptance_end_datetime",
        "order": "ASC",
    }
    if area_name:
        params["target_area_search"] = area_name

    with httpx.Client(timeout=30.0) as client:
        try:
            resp = client.get(f"{BASE_URL}/subsidies", params=params)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            logger.warning("Jグランツ API エラー keyword=%s area=%s: %s",
                           keyword, area_name, e)
            return []

    items = resp.json().get("result", []) or []
    total = resp.json().get("metadata", {}).get("resultset", {}).get("count", len(items))
    logger.info("Jグランツ: keyword=%s area=%s → %d件（全%d件）",
                keyword, area_name or "全国", len(items), total)
    return [_parse_subsidy(item) for item in items]


def search_by_area(
    area_name: str,
    acceptance: str = "1",
    keywords: list[str] | None = None,
) -> Iterator[JGrantsSubsidy]:
    """
    指定エリアの補助金を複数キーワードで検索してイテレートする（重複除去済み）。

    Args:
        area_name: 都道府県名・市区町村名（例: "大阪府", "横浜市"）
        acceptance: "0"=全て, "1"=募集中のみ
        keywords: 検索キーワードリスト（省略時は DEFAULT_KEYWORDS を使用）
    """
    if keywords is None:
        keywords = DEFAULT_KEYWORDS

    seen_ids: set[str] = set()
    for kw in keywords:
        for subsidy in search(kw, area_name=area_name, acceptance=acceptance):
            if subsidy.subsidy_id not in seen_ids:
                seen_ids.add(subsidy.subsidy_id)
                yield subsidy
        time.sleep(RATE_SLEEP)


def get_detail(subsidy_id: str) -> dict | None:
    """補助金IDで詳細情報を取得する。"""
    url = f"{BASE_URL}/subsidies/id/{subsidy_id}"
    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(url)
            resp.raise_for_status()
            return resp.json().get("result")
    except httpx.HTTPError as e:
        logger.warning("Jグランツ 詳細取得エラー id=%s: %s", subsidy_id, e)
        return None
