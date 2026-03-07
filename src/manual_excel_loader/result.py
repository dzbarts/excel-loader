from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")

@dataclass(frozen=True)
class Ok(Generic[T]):
    """Успешный результат валидации."""
    value: T

@dataclass(frozen=True)
class Err:
    """Неуспешный результат с описанием причины."""
    message: str

# Тип-псевдоним: результат это либо Ok, либо Err
ValidationResult = Ok[T] | Err