"""Unit tests for CacheClient: graceful degradation, no-cache mode, flush, key format."""

from __future__ import annotations

import hashlib
import json
from unittest.mock import MagicMock, patch, call

import pytest

from src.cache.client import CacheClient, _KNOWN_PREFIXES, CACHE_TTL_YAML


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_redis_module():
    """Patch the redis module so no real Redis connection is made."""
    mock_conn = MagicMock()
    mock_conn.ping.return_value = True
    mock_conn.get.return_value = None
    mock_conn.set.return_value = True
    mock_conn.delete.return_value = 1
    mock_conn.scan.return_value = (0, [])

    mock_pool = MagicMock()

    with patch.dict("sys.modules", {"redis": MagicMock(
        ConnectionPool=MagicMock(return_value=mock_pool),
        Redis=MagicMock(return_value=mock_conn),
        RedisError=Exception,
    )}):
        yield mock_conn, mock_pool


@pytest.fixture
def connected_client(mock_redis_module):
    """CacheClient with mocked Redis connection that succeeds."""
    client = CacheClient(host="localhost", port=6379)
    assert client._available is True
    return client, mock_redis_module[0]


# ---------------------------------------------------------------------------
# no_cache mode
# ---------------------------------------------------------------------------

class TestNoCacheMode:
    def test_get_returns_none(self):
        client = CacheClient(no_cache=True)
        assert client.get("yaml", "some-key") is None

    def test_set_returns_false(self):
        client = CacheClient(no_cache=True)
        assert client.set("yaml", "some-key", b"data", ttl=60) is False

    def test_delete_returns_false(self):
        client = CacheClient(no_cache=True)
        assert client.delete("yaml", "some-key") is False

    def test_flush_returns_zero(self):
        client = CacheClient(no_cache=True)
        assert client.flush_all_prefixes() == 0

    def test_available_is_false(self):
        client = CacheClient(no_cache=True)
        assert client._available is False

    def test_stats_still_accessible(self):
        client = CacheClient(no_cache=True)
        stats = client.get_stats()
        assert stats is not None
        summary = stats.summary()
        assert isinstance(summary, dict)


# ---------------------------------------------------------------------------
# Graceful degradation on ConnectionError
# ---------------------------------------------------------------------------

class TestGracefulDegradation:
    def test_init_failure_sets_unavailable(self):
        """ConnectionError during ping must leave client in degraded mode."""
        with patch.dict("sys.modules", {"redis": MagicMock(
            ConnectionPool=MagicMock(return_value=MagicMock()),
            Redis=MagicMock(side_effect=Exception("connection refused")),
            RedisError=Exception,
        )}):
            client = CacheClient(host="nonexistent-host", port=6379)
        assert client._available is False

    def test_get_noop_when_unavailable(self):
        with patch.dict("sys.modules", {"redis": MagicMock(
            ConnectionPool=MagicMock(return_value=MagicMock()),
            Redis=MagicMock(side_effect=Exception("refused")),
            RedisError=Exception,
        )}):
            client = CacheClient()
        assert client.get("llm", "key") is None

    def test_redis_error_during_get_disables_client(self, connected_client):
        """RedisError during GET must flip _available to False."""
        client, mock_conn = connected_client
        mock_conn.get.side_effect = Exception("timeout")
        result = client.get("yaml", "anything")
        assert result is None
        assert client._available is False

    def test_redis_error_during_set_disables_client(self, connected_client):
        client, mock_conn = connected_client
        mock_conn.set.side_effect = Exception("timeout")
        result = client.set("yaml", "anything", b"v", ttl=60)
        assert result is False
        assert client._available is False

    def test_stats_records_miss_on_degraded(self):
        with patch.dict("sys.modules", {"redis": MagicMock(
            ConnectionPool=MagicMock(return_value=MagicMock()),
            Redis=MagicMock(side_effect=Exception("refused")),
            RedisError=Exception,
        )}):
            client = CacheClient()
        client.get("yaml", "key1")
        client.get("yaml", "key2")
        summary = client.get_stats().summary()
        assert summary.get("yaml", {}).get("misses", 0) >= 2


# ---------------------------------------------------------------------------
# _make_key format
# ---------------------------------------------------------------------------

class TestMakeKey:
    def setup_method(self):
        self.client = CacheClient(no_cache=True)

    def test_key_format_prefix_colon_hash(self):
        key = self.client._make_key("yaml", "test-input")
        parts = key.split(":")
        assert len(parts) == 2
        assert parts[0] == "yaml"
        assert len(parts[1]) == 16

    def test_key_hash_is_sha256_16(self):
        raw = "test-input"
        expected_hash = hashlib.sha256(raw.encode()).hexdigest()[:16]
        key = self.client._make_key("yaml", raw)
        assert key == f"yaml:{expected_hash}"

    def test_key_deterministic(self):
        k1 = self.client._make_key("llm", "same-input")
        k2 = self.client._make_key("llm", "same-input")
        assert k1 == k2

    def test_different_prefixes_produce_different_keys(self):
        k1 = self.client._make_key("yaml", "same-input")
        k2 = self.client._make_key("llm", "same-input")
        assert k1 != k2
        assert k1.startswith("yaml:")
        assert k2.startswith("llm:")

    def test_list_input_serialized_consistently(self):
        k1 = self.client._make_key("emb", ["a", "b", "c"])
        k2 = self.client._make_key("emb", ["a", "b", "c"])
        assert k1 == k2
        assert k1.startswith("emb:")


# ---------------------------------------------------------------------------
# flush_all_prefixes
# ---------------------------------------------------------------------------

class TestFlushAllPrefixes:
    def test_flush_deletes_keys_for_all_prefixes(self, connected_client):
        client, mock_conn = connected_client
        fake_keys_yaml = [b"yaml:abc", b"yaml:def"]
        fake_keys_llm = [b"llm:xyz"]

        scan_results = {
            "yaml:*": [(0, fake_keys_yaml)],
            "llm:*": [(0, fake_keys_llm)],
            "emb:*": [(0, [])],
            "dedup:*": [(0, [])],
        }
        call_counts: dict[str, int] = {p: 0 for p in _KNOWN_PREFIXES}

        def mock_scan(cursor=0, match="", count=100):
            prefix = match.split(":")[0]
            idx = call_counts[prefix]
            call_counts[prefix] += 1
            results = scan_results.get(match, [(0, [])])
            return results[min(idx, len(results) - 1)]

        mock_conn.scan.side_effect = mock_scan
        deleted = client.flush_all_prefixes()
        assert deleted == 3  # 2 yaml + 1 llm

    def test_flush_returns_zero_when_no_keys(self, connected_client):
        client, mock_conn = connected_client
        mock_conn.scan.return_value = (0, [])
        assert client.flush_all_prefixes() == 0

    def test_flush_returns_zero_when_unavailable(self):
        client = CacheClient(no_cache=True)
        assert client.flush_all_prefixes() == 0


# ---------------------------------------------------------------------------
# get / set round-trip
# ---------------------------------------------------------------------------

class TestGetSet:
    def test_set_then_get_returns_value(self, connected_client):
        client, mock_conn = connected_client
        payload = b'{"primary_category": "Dairy"}'
        mock_conn.get.return_value = payload

        client.set("llm", "key1", payload, ttl=CACHE_TTL_YAML)
        result = client.get("llm", "key1")
        assert result == payload

    def test_get_miss_returns_none(self, connected_client):
        client, mock_conn = connected_client
        mock_conn.get.return_value = None
        assert client.get("llm", "nonexistent") is None

    def test_hit_recorded_in_stats(self, connected_client):
        client, mock_conn = connected_client
        mock_conn.get.return_value = b"hit-value"
        client.get("emb", "key")
        summary = client.get_stats().summary()
        assert summary.get("emb", {}).get("hits", 0) == 1

