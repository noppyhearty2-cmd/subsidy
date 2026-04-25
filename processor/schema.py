from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, Field, field_validator


class SubsidyRecord(BaseModel):
    municipality_id: str
    source_url: str
    title: str
    target: str
    amount: str | None = None
    deadline: str | None = None  # ISO 8601 (YYYY-MM-DD) or "随時" or None
    key_points: Annotated[list[str], Field(min_length=1, max_length=5)]
    tags: list[str] = Field(default_factory=list)
    summary_ja: str
    article_body: str
    scraped_at: datetime
    content_hash: str
    is_active: bool = True

    @field_validator("key_points")
    @classmethod
    def validate_key_points(cls, v: list[str]) -> list[str]:
        return [p for p in v if p.strip()]

    @field_validator("deadline")
    @classmethod
    def validate_deadline(cls, v: str | None) -> str | None:
        if v is None or v == "随時" or v == "":
            return v or None
        # ISO 8601 形式か緩やかにチェック
        import re
        if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
            return v
        # 日付として解釈できない場合もそのまま通す（"応相談" 等）
        return v
