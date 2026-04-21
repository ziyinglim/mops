"""
Chinese → English translation wrapper.
Uses deep_translator (Google Translate, no API key required).
Falls back to returning original text if translation fails.
"""
import logging
import re
from functools import lru_cache

logger = logging.getLogger(__name__)

try:
    from deep_translator import GoogleTranslator
    _TRANSLATOR_AVAILABLE = True
except ImportError:
    logger.warning("deep_translator not installed — translation disabled. Run: pip install deep-translator")
    _TRANSLATOR_AVAILABLE = False


def translate_zh_to_en(text: str) -> str:
    """Translate Chinese text to English. Returns original if translation unavailable."""
    if not text or not _needs_translation(text):
        return text
    if not _TRANSLATOR_AVAILABLE:
        return text
    try:
        return _translate_cached(text)
    except Exception as exc:
        logger.warning("Translation failed for '%s...': %s", text[:40], exc)
        return text


@lru_cache(maxsize=1024)
def _translate_cached(text: str) -> str:
    translator = GoogleTranslator(source="zh-TW", target="en")
    return translator.translate(text)


def translate_record(record: dict, fields_to_translate: list[str]) -> dict:
    """
    Add `_en` suffixed keys for specified fields in a record.
    Original fields are preserved unchanged.
    """
    result = dict(record)
    for field in fields_to_translate:
        if field in record and record[field]:
            result[f"{field}_en"] = translate_zh_to_en(str(record[field]))
    return result


def _needs_translation(text: str) -> bool:
    """Returns True if text contains CJK characters."""
    return bool(re.search(r"[\u4e00-\u9fff\u3400-\u4dbf]", text))
