"""Unit tests for UC2 kafka_to_pg."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.uc2_observability import kafka_to_pg as k2p


class TestSafeJson:
    def test_valid(self):
        assert k2p._safe_json({"a": 1}) == '{"a": 1}'

    def test_fallback(self):
        class Unser:
            pass
        assert k2p._safe_json({"a": Unser()}) == "{}"


class TestTsToEpoch:
    def test_none_returns_now(self):
        v = k2p._ts_to_epoch(None)
        assert isinstance(v, float)
        assert v > 0

    def test_float_passthrough(self):
        assert k2p._ts_to_epoch(123.45) == 123.45

    def test_int_cast(self):
        assert k2p._ts_to_epoch(100) == 100.0

    def test_iso_string(self):
        v = k2p._ts_to_epoch("2024-01-01T00:00:00+00:00")
        assert v > 0

    def test_zulu(self):
        v = k2p._ts_to_epoch("2024-01-01T00:00:00Z")
        assert v > 0

    def test_bad_string_falls_back(self):
        v = k2p._ts_to_epoch("not-a-date")
        assert v > 0


class TestHandlers:
    def test_audit(self):
        cur = MagicMock()
        k2p._handle_audit(cur, {"run_id": "r", "source": "s", "event_type": "run_started",
                                 "status": "ok", "ts": 1.0})
        assert cur.execute.called

    def test_block_trace(self):
        cur = MagicMock()
        k2p._handle_block_trace(cur, {"run_id": "r", "source": "s", "block": "b",
                                       "event_type": "block_end", "rows_in": 1, "rows_out": 1,
                                       "null_rates": {"a": 0.1}, "duration_ms": 10, "dq_score": 0.9,
                                       "block_seq": 1, "ts": 1.0})
        assert cur.execute.called

    def test_quarantine(self):
        cur = MagicMock()
        k2p._handle_quarantine(cur, {"run_id": "r", "source": "s", "row_hash": "h",
                                      "reason": "x", "row_data": {"a": 1}, "ts": 1.0})
        assert cur.execute.called

    def test_dedup(self):
        cur = MagicMock()
        k2p._handle_dedup(cur, {"run_id": "r", "source": "s", "cluster_id": "c",
                                 "canonical": "cn", "members": ["a", "b"],
                                 "merge_decisions": {"a": "x"}, "ts": 1.0})
        assert cur.execute.called

    def test_handlers_registry(self):
        assert "run_started" in k2p._HANDLERS
        assert "block_end" in k2p._HANDLERS
        assert "dedup_cluster" in k2p._HANDLERS


class TestProducer:
    def test_emit_event_never_raises(self):
        # Reset module singleton
        k2p._producer = None
        with patch("kafka.KafkaProducer", side_effect=Exception("nope")):
            k2p.emit_event({"event_type": "x"})

    def test_emit_event_sends(self):
        fake = MagicMock()
        k2p._producer = fake
        k2p.emit_event({"event_type": "run_started"})
        assert fake.send.called
        k2p._producer = None

    def test_get_producer_lazy(self):
        k2p._producer = None
        with patch("kafka.KafkaProducer", return_value=MagicMock()) as kp:
            p = k2p._get_producer()
            p2 = k2p._get_producer()
        assert kp.call_count == 1
        assert p is p2
        k2p._producer = None
