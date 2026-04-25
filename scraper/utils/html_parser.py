import re

from bs4 import BeautifulSoup


def clean_text(html: str | bytes, main_selector: str = "") -> str:
    """HTMLから本文テキストを抽出。main_selector が指定されていればその要素内に絞る。"""
    soup = BeautifulSoup(html, "lxml")

    # スクリプト・スタイル・ナビ等を除去
    for tag in soup(["script", "style", "nav", "header", "footer", "aside", "noscript"]):
        tag.decompose()

    if main_selector:
        node = soup.select_one(main_selector)
        target = node if node else soup.body or soup
    else:
        target = soup.body or soup

    text = target.get_text(separator="\n")
    return _clean(text)


def extract_links(html: str | bytes, base_url: str, link_selector: str) -> list[str]:
    """指定セレクタに一致するリンクURLを絶対URLで返す。"""
    from urllib.parse import urljoin
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.select(link_selector):
        href = a.get("href", "")
        if href and not href.startswith("#"):
            links.append(urljoin(base_url, href))
    return links


def extract_title(html: str | bytes, title_selector: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    node = soup.select_one(title_selector)
    if node:
        return node.get_text(strip=True)
    if soup.title:
        return soup.title.get_text(strip=True)
    return ""


def _clean(text: str) -> str:
    text = re.sub(r"\n{3,}", "\n\n", text)
    lines = [line.strip() for line in text.splitlines()]
    lines = [l for l in lines if l]
    return "\n".join(lines)
