import logging
import re
from pathlib import Path

from processor.schema import SubsidyRecord

logger = logging.getLogger(__name__)

CONTENT_DIR = Path(__file__).parent.parent / "site" / "src" / "content" / "subsidies"


class MarkdownWriter:
    def __init__(self, content_dir: Path = CONTENT_DIR):
        self._content_dir = content_dir

    def write(self, record: SubsidyRecord) -> Path:
        out_dir = self._content_dir / record.municipality_id
        out_dir.mkdir(parents=True, exist_ok=True)

        slug = _make_slug(record.source_url)
        date_str = record.scraped_at.strftime("%Y-%m-%d")
        filename = f"{date_str}-{slug}.md"
        output_path = out_dir / filename

        content = self._render(record)
        output_path.write_text(content, encoding="utf-8")
        logger.info("Markdown 書き出し: %s", output_path)
        return output_path

    def _render(self, r: SubsidyRecord) -> str:
        frontmatter = self._build_frontmatter(r)
        return f"{frontmatter}\n{r.article_body}\n"

    @staticmethod
    def _build_frontmatter(r: SubsidyRecord) -> str:
        tags_yaml = "[" + ", ".join(f'"{t}"' for t in r.tags) + "]"
        kp_yaml = "\n".join(f'  - "{p}"' for p in r.key_points)
        deadline = r.deadline or ""
        amount = r.amount or ""
        return (
            "---\n"
            f'title: "{_escape(r.title)}"\n'
            f"municipality: {r.municipality_id}\n"
            f'target: "{_escape(r.target)}"\n'
            f'amount: "{_escape(amount)}"\n'
            f'deadline: "{deadline}"\n'
            f"tags: {tags_yaml}\n"
            f"key_points:\n{kp_yaml}\n"
            f'source_url: "{r.source_url}"\n'
            f'summary_ja: "{_escape(r.summary_ja)}"\n'
            f'scraped_at: "{r.scraped_at.isoformat()}"\n'
            f"is_active: {str(r.is_active).lower()}\n"
            f'content_hash: "{r.content_hash}"\n'
            "---\n"
        )


def _make_slug(url: str) -> str:
    from urllib.parse import urlparse
    path = urlparse(url).path
    slug = re.sub(r"[^a-zA-Z0-9぀-鿿]", "-", path)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug[:50] or "subsidy"


def _escape(text: str) -> str:
    return text.replace('"', '\\"').replace("\n", " ")
