"""
Parses numbered field blocks from MOPS announcement Statement columns.
Input:  "1.Fund name: Inflexion VII\n2.Date: 2026/02/11\n5.Amount: EUR 25,000,000"
Output: {1: "Fund name: Inflexion VII", 2: "Date: 2026/02/11", 5: "Amount: EUR 25,000,000"}
"""
import re


def parse_statement_fields(text: str) -> dict[int, str]:
    """
    Split statement text on numbered field markers (e.g. '1.', '2.', '10.').
    Returns a dict mapping field number -> field content string.
    """
    if not text:
        return {}

    # Normalise: collapse excessive whitespace, unify line endings
    text = re.sub(r"\r\n|\r", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)

    # Split on patterns like: "\n1." or "1." at start of string or after newline
    # Look-ahead keeps the number in the captured group
    parts = re.split(r"(?:^|\n)(\d{1,2})\.\s*", text, flags=re.MULTILINE)

    fields: dict[int, str] = {}

    # parts alternates: [pre-text, num, content, num, content, ...]
    # index 0 is text before first field number; skip it
    i = 1
    while i < len(parts) - 1:
        try:
            num = int(parts[i])
            content = parts[i + 1].strip()
            fields[num] = content
        except (ValueError, IndexError):
            pass
        i += 2

    return fields
