from __future__ import annotations

import csv
from typing import Iterable

from .base import BaseWriter, FileWriterConfig


class CsvFileWriter(BaseWriter):

    def __init__(self, config: FileWriterConfig) -> None:
        self._config = config

    def write(self, headers: list[str], rows: Iterable[tuple]) -> None:
        with open(
            self._config.output_path,
            "w",
            newline="",           # переносы строк отдаём csv-модулю
            encoding=self._config.encoding,
        ) as fh:
            writer = csv.writer(
                fh,
                delimiter=self._config.delimiter,
                quoting=csv.QUOTE_MINIMAL,
            )
            writer.writerow(headers)
            writer.writerows(rows)