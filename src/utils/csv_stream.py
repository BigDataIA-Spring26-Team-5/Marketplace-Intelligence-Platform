from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "10000"))


class CsvStreamReader:
    """Streaming CSV reader with chunked iteration."""

    def __init__(
        self,
        file_path: str | Path,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        encoding: str = "utf-8",
        delimiter: str = ",",
    ):
        self.file_path = Path(file_path)
        self.chunk_size = chunk_size
        self.encoding = encoding
        self.delimiter = delimiter

        if not self.file_path.exists():
            raise FileNotFoundError(f"Source file not found: {file_path}")

    def __iter__(self) -> Iterator[pd.DataFrame]:
        """Yield DataFrame chunks."""
        for chunk in pd.read_csv(
            self.file_path,
            chunksize=self.chunk_size,
            encoding=self.encoding,
            delimiter=self.delimiter,
        ):
            yield chunk

    def get_total_rows(self) -> int:
        """Count total rows without loading full file."""
        total = 0
        for chunk in pd.read_csv(
            self.file_path,
            chunksize=self.chunk_size,
            encoding=self.encoding,
            delimiter=self.delimiter,
            usecols=[0],
        ):
            total += len(chunk)
        return total

    def get_chunks_count(self) -> int:
        """Calculate number of chunks without processing full file."""
        total_rows = self.get_total_rows()
        return (total_rows + self.chunk_size - 1) // self.chunk_size

    @property
    def headers(self) -> list[str]:
        """Get column headers from CSV."""
        df = pd.read_csv(
            self.file_path,
            nrows=0,
            encoding=self.encoding,
            delimiter=self.delimiter,
        )
        return df.columns.tolist()