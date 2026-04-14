from __future__ import annotations

import csv
from typing import Iterable

from .base import BaseWriter, FileWriterConfig


class CsvFileWriter(BaseWriter):

    def __init__(self, config: FileWriterConfig) -> None:
        self._config = config

    def write(self, headers: list[str], rows: Iterable[tuple]) -> int:
        writer = csv.writer(
            self._config.output_stream,
            delimiter=self._config.delimiter,
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writerow(headers)
        total = 0
        for row in rows:
            writer.writerow(row)
            total += 1
        return total