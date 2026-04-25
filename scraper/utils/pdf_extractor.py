import io
import re


def extract_text(pdf_bytes: bytes) -> str:
    """PDFバイナリからテキストを抽出。pdfplumber 優先、失敗時は pymupdf にフォールバック。"""
    text = _try_pdfplumber(pdf_bytes)
    if not text.strip():
        text = _try_pymupdf(pdf_bytes)
    return _clean(text)


def _try_pdfplumber(pdf_bytes: bytes) -> str:
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = [page.extract_text() or "" for page in pdf.pages]
        return "\n".join(pages)
    except Exception:
        return ""


def _try_pymupdf(pdf_bytes: bytes) -> str:
    try:
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages = [page.get_text() for page in doc]
        return "\n".join(pages)
    except Exception:
        return ""


def _clean(text: str) -> str:
    # ページ番号・ヘッダー行を除去
    lines = text.splitlines()
    cleaned = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if re.fullmatch(r"\d+", stripped):
            continue
        if len(stripped) < 3:
            continue
        cleaned.append(stripped)
    return "\n".join(cleaned)
