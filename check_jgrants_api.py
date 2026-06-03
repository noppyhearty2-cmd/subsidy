import sys, httpx, re
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

r = httpx.get("https://developers.digital.go.jp/documents/jgrants/api/", timeout=15, follow_redirects=True)
print(f"status: {r.status_code}, len: {len(r.text)}")
html = r.text

# YAMLリンクを探す
yaml_links = re.findall(r'href=["\']([^"\']*\.yaml[^"\']*)["\']', html)
print(f"YAMLリンク: {yaml_links}")

# 外部リンクを探す
links = re.findall(r'href=["\']([^"\']+)["\']', html)
jgrants_links = [l for l in links if "jgrant" in l.lower() or "api" in l.lower() or "yaml" in l.lower()]
print(f"関連リンク: {jgrants_links[:20]}")

# keyword や acceptance を含む部分
for kw in ["keyword", "acceptance", "subsidy_keywords", "request", "curl", "sample", "認証", "APIキー"]:
    idx = html.lower().find(kw.lower())
    if idx >= 0:
        snippet = html[max(0, idx-30):idx+120].replace("\n", " ")
        print(f"[{kw}]: ...{snippet}...")

# ページ全体の最初の2000文字（JS以外）
text_only = re.sub(r"<[^>]+>", " ", html)
text_only = re.sub(r"\s+", " ", text_only)
print("\n--- ページテキスト（先頭2000字）---")
print(text_only[:2000])
