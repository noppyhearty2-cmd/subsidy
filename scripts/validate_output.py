"""
生成された Markdown ファイルの frontmatter スキーマを検証する。
GitHub Actions のデプロイ前ゲートとして使用。

使い方:
  python scripts/validate_output.py site/src/content/subsidies/
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import frontmatter

REQUIRED_FIELDS = ["title", "municipality", "target", "source_url", "scraped_at", "is_active"]


def validate_dir(content_dir: Path) -> tuple[int, int]:
    ok = fail = 0
    for md_file in sorted(content_dir.rglob("*.md")):
        errors = validate_file(md_file)
        if errors:
            print(f"[FAIL] {md_file}")
            for e in errors:
                print(f"       {e}")
            fail += 1
        else:
            ok += 1
    return ok, fail


def validate_file(path: Path) -> list[str]:
    errors = []
    try:
        post = frontmatter.load(str(path))
    except Exception as e:
        return [f"frontmatter 解析失敗: {e}"]

    for field in REQUIRED_FIELDS:
        if field not in post.metadata:
            errors.append(f"必須フィールドなし: {field}")

    if not post.content.strip():
        errors.append("記事本文が空です")

    title = post.metadata.get("title", "")
    if title and "【" not in title:
        errors.append(f"タイトル形式不正（【年】が含まれていない）: {title[:50]}")

    if "source_url" in post.metadata:
        url = post.metadata["source_url"]
        if not str(url).startswith("http"):
            errors.append(f"source_url が URL 形式ではありません: {url}")

    return errors


def main():
    if len(sys.argv) < 2:
        print("使い方: python scripts/validate_output.py <content_dir>")
        sys.exit(1)

    content_dir = Path(sys.argv[1])
    if not content_dir.exists():
        print(f"ディレクトリが見つかりません: {content_dir}")
        sys.exit(1)

    ok, fail = validate_dir(content_dir)
    print(f"\n検証結果: {ok} 件OK / {fail} 件NG")

    if fail > 0:
        sys.exit(1)
    print("全ファイルの検証に成功しました。")


if __name__ == "__main__":
    main()
