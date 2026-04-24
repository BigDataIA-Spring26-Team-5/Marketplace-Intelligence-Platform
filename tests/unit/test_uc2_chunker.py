"""Unit tests for UC2 chunker."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def patched_chunker(tmp_path, monkeypatch):
    fake_model = MagicMock()
    fake_model.encode.return_value = MagicMock(tolist=lambda: [[0.1, 0.2]])
    fake_chroma = MagicMock()
    fake_collection = MagicMock()
    fake_chroma.get_or_create_collection.return_value = fake_collection

    with patch("sentence_transformers.SentenceTransformer", return_value=fake_model), \
         patch("chromadb.HttpClient", return_value=fake_chroma):
        from src.uc2_observability import chunker as ch
        monkeypatch.setattr(ch, "CURSOR_FILE", tmp_path / "cursor.txt")
        c = ch.Chunker()
        c.collection = fake_collection
        c.model = fake_model
        yield ch, c, fake_collection, fake_model


class TestFormatEvent:
    def test_basic_format(self):
        from src.uc2_observability.chunker import _format_event
        row = {"id": 1, "run_id": "r1", "source": "OFF", "event_type": "block_end",
               "status": "ok", "ts": 123.0, "payload": '{"block": "clean", "rows_in": 10}'}
        text = _format_event(row)
        assert "block_end" in text
        assert "r1" in text
        assert "OFF" in text
        assert "block: clean" in text
        assert "rows_in: 10" in text

    def test_bad_json_payload(self):
        from src.uc2_observability.chunker import _format_event
        row = {"id": 1, "run_id": "r1", "source": "s", "event_type": "e",
               "status": None, "ts": 0, "payload": "not-json"}
        text = _format_event(row)
        assert "e" in text

    def test_dict_payload_passthrough(self):
        from src.uc2_observability.chunker import _format_event
        row = {"id": 1, "run_id": "r", "source": "s", "event_type": "x",
               "ts": 0, "payload": {"members": ["a", "b"]}}
        text = _format_event(row)
        assert "members" in text

    def test_missing_fields(self):
        from src.uc2_observability.chunker import _format_event
        text = _format_event({"id": 1})
        assert "unknown" in text


class TestCursor:
    def test_read_missing(self, tmp_path, monkeypatch):
        from src.uc2_observability import chunker as ch
        monkeypatch.setattr(ch, "CURSOR_FILE", tmp_path / "no.txt")
        assert ch._read_cursor() == 0

    def test_write_then_read(self, tmp_path, monkeypatch):
        from src.uc2_observability import chunker as ch
        monkeypatch.setattr(ch, "CURSOR_FILE", tmp_path / "c.txt")
        ch._write_cursor(42)
        assert ch._read_cursor() == 42

    def test_read_bad_content(self, tmp_path, monkeypatch):
        from src.uc2_observability import chunker as ch
        p = tmp_path / "c.txt"
        p.write_text("not-int")
        monkeypatch.setattr(ch, "CURSOR_FILE", p)
        assert ch._read_cursor() == 0


class TestChunker:
    def test_run_once_no_new_events(self, patched_chunker):
        ch, c, col, model = patched_chunker
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        with patch.object(ch.psycopg2, "connect", return_value=mock_conn):
            assert c.run_once() == 0

    def test_run_once_with_events(self, patched_chunker):
        ch, c, col, model = patched_chunker
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [
            {"id": 1, "run_id": "r", "source": "s", "event_type": "e",
             "status": "ok", "ts": 1.0, "payload": "{}"},
            {"id": 2, "run_id": "r", "source": "s", "event_type": "e2",
             "status": "ok", "ts": 2.0, "payload": "{}"},
        ]
        model.encode.return_value = MagicMock(tolist=lambda: [[0.1], [0.2]])
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        with patch.object(ch.psycopg2, "connect", return_value=mock_conn):
            n = c.run_once()
        assert n == 2
        assert col.upsert.called

    def test_upsert_empty_batch_noop(self, patched_chunker):
        ch, c, col, _ = patched_chunker
        c._upsert_batch([])
        assert not col.upsert.called
