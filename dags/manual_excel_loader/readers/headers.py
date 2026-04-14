"""
readers/headers.py
==================
Общая валидация заголовков — используется и Excel-, и CSV-ридером.
"""

from __future__ import annotations

import re

from ..exceptions import HeaderValidationError

_VALID_HEADER = re.compile(r"^[a-z0-9_]+$")


def read_headers_raw(raw: list) -> list[str]:
    """Нормализовать заголовок без проверки символов.

    Используется когда skip_header_validation=True.
    """
    last_non_none = max(
        (i for i, v in enumerate(raw) if v is not None),
        default=-1,
    )
    if last_non_none == -1:
        raise HeaderValidationError("header row is empty or all cells are None")
    return [str(v).lower().strip() for v in raw[: last_non_none + 1]]


def validate_headers(raw: list) -> list[str]:
    """Валидировать и нормализовать заголовочную строку.

    Raises:
        HeaderValidationError: пустые заголовки, недопустимые символы,
            дубликаты.
    """
    headers = read_headers_raw(raw)

    for h in headers:
        if not _VALID_HEADER.fullmatch(h):
            raise HeaderValidationError(
                f"column name '{h}' contains invalid characters. "
                "Only lowercase Latin letters, digits and underscores are allowed."
            )

    if len(headers) != len(set(headers)):
        seen: set[str] = set()
        duplicates = [h for h in headers if h in seen or seen.add(h)]  # type: ignore[func-returns-value]
        raise HeaderValidationError(
            f"duplicate column names are not allowed: {duplicates}"
        )

    return headers
