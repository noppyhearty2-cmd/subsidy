import time

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    return isinstance(exc, (httpx.TimeoutException, httpx.ConnectError))


@retry(
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError, httpx.HTTPStatusError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    reraise=True,
)
def get(url: str, rate_limit: float = 1.0, **kwargs) -> httpx.Response:
    time.sleep(rate_limit)
    with httpx.Client(headers=DEFAULT_HEADERS, timeout=30, follow_redirects=True) as client:
        resp = client.get(url, **kwargs)
        resp.raise_for_status()
        return resp


def get_bytes(url: str, rate_limit: float = 1.0, max_bytes: int = 10 * 1024 * 1024) -> bytes:
    """PDF等のバイナリを取得。max_bytes を超える場合は ValueError を送出。"""
    time.sleep(rate_limit)
    with httpx.Client(headers=DEFAULT_HEADERS, timeout=60, follow_redirects=True) as client:
        with client.stream("GET", url) as resp:
            resp.raise_for_status()
            content_length = int(resp.headers.get("content-length", 0))
            if content_length > max_bytes:
                raise ValueError(f"File too large: {content_length} bytes (max {max_bytes})")
            data = b""
            for chunk in resp.iter_bytes(chunk_size=8192):
                data += chunk
                if len(data) > max_bytes:
                    raise ValueError(f"File too large (exceeded {max_bytes} bytes during download)")
    return data
