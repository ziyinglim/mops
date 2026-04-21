"""
Shared async HTTP client for direct POST requests to emops endpoints.
Tries each TYPEK value (listed / OTC / emerging) until one returns content.
"""
import logging
import warnings
import httpx

warnings.filterwarnings("ignore", message="Unverified HTTPS request")

logger = logging.getLogger(__name__)

EMOPS_HOST = "https://emops.twse.com.tw"

# TWSE stock type codes — try sii (main board) first, then otc, then rotc
TYPEK_OPTIONS = ["sii", "otc", "rotc", "co"]

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": f"{EMOPS_HOST}/server-java/t58query",
    "Origin": EMOPS_HOST,
    "Content-Type": "application/x-www-form-urlencoded",
}


async def post_emops(path: str, stock_code: str, extra: dict = None) -> str | None:
    """
    POST to an emops endpoint, trying each TYPEK until we get a non-empty response.
    Returns HTML string or None if all attempts fail.
    """
    url = EMOPS_HOST + path
    base_data = {"co_id": stock_code, **(extra or {})}

    async with httpx.AsyncClient(headers=_HEADERS, timeout=30, follow_redirects=True, verify=False) as client:
        # Brief visit to main page to pick up any session cookies
        try:
            await client.get(f"{EMOPS_HOST}/server-java/t58query", extensions={"sni_hostname": "emops.twse.com.tw"})
        except Exception:
            pass

        for typek in TYPEK_OPTIONS:
            data = {**base_data, "TYPEK": typek}
            try:
                resp = await client.post(url, data=data)
                resp.raise_for_status()
                html = resp.text
                # Reject empty or error pages
                if html and len(html) > 200 and "error" not in html[:200].lower():
                    logger.info("POST %s co_id=%s TYPEK=%s → %d chars", path, stock_code, typek, len(html))
                    return html
                logger.debug("Empty/error response for TYPEK=%s, trying next", typek)
            except Exception as exc:
                logger.warning("POST failed for TYPEK=%s: %s", typek, exc)

    logger.error("All TYPEK attempts failed for %s %s", path, stock_code)
    return None
