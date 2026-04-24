"""Unit tests for CsvStreamReader."""
from __future__ import annotations

import pytest

from src.utils.csv_stream import CsvStreamReader, DEFAULT_CHUNK_SIZE, NULL_SENTINELS


@pytest.fixture
def sample_csv(tmp_path):
    p = tmp_path / "f.csv"
    p.write_text("a,b\n1,x\n2,y\n3,z\n4,w\n5,v\n")
    return p


class TestInit:
    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            CsvStreamReader(tmp_path / "nope.csv")

    def test_defaults(self, sample_csv):
        r = CsvStreamReader(sample_csv)
        assert r.chunk_size == DEFAULT_CHUNK_SIZE
        assert r.delimiter == ","
        assert r.na_values == NULL_SENTINELS

    def test_custom_na_values(self, sample_csv):
        r = CsvStreamReader(sample_csv, na_values=["X"])
        assert r.na_values == ["X"]


class TestIteration:
    def test_chunks(self, sample_csv):
        r = CsvStreamReader(sample_csv, chunk_size=2)
        chunks = list(r)
        assert len(chunks) == 3
        assert len(chunks[0]) == 2
        assert len(chunks[2]) == 1

    def test_single_chunk(self, sample_csv):
        r = CsvStreamReader(sample_csv, chunk_size=100)
        chunks = list(r)
        assert len(chunks) == 1
        assert len(chunks[0]) == 5


class TestCounts:
    def test_get_total_rows(self, sample_csv):
        r = CsvStreamReader(sample_csv, chunk_size=2)
        assert r.get_total_rows() == 5

    def test_get_chunks_count(self, sample_csv):
        r = CsvStreamReader(sample_csv, chunk_size=2)
        assert r.get_chunks_count() == 3

    def test_headers(self, sample_csv):
        r = CsvStreamReader(sample_csv)
        assert r.headers == ["a", "b"]


class TestNullSentinels:
    def test_nulls_detected(self, tmp_path):
        p = tmp_path / "n.csv"
        p.write_text("a,b\n1,NULL\n2,n/a\n")
        r = CsvStreamReader(p, chunk_size=10)
        df = next(iter(r))
        assert df["b"].isna().sum() == 2
