from __future__ import annotations
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class Ok(Generic[T]):
    """Успешный результат валидации одной ячейки."""
    value: T


@dataclass(frozen=True)
class Err:
    """Неуспешный результат с описанием причины."""
    message: str


# Псевдоним для аннотации возвращаемого типа валидаторов
CellResult = Ok[T] | Err