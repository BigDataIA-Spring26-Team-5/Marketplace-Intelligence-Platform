"""Unit tests for kafka_gcs_sink consumer."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.consumers import kafka_gcs_sink as sink


class TestGcsClient:
    def test_gcs_client(self):
        with patch("src.consumers.kafka_gcs_sink.boto3.client") as m:
            sink.gcs_client()
        assert m.called


class TestFlush:
    def test_flush_writes_jsonl(self):
        client = MagicMock()
        sink.flush(client, [{"a": 1}, {"b": 2}], "off", "2024/01/01", 5)
        args, kwargs = client.put_object.call_args
        assert kwargs["Key"] == "off/2024/01/01/part_0005.jsonl"
        assert kwargs["ContentType"] == "application/x-ndjson"

    def test_flush_empty_buffer(self):
        client = MagicMock()
        sink.flush(client, [], "x", "d", 0)
        assert client.put_object.called


class TestMain:
    def test_main_consumes_and_flushes(self):
        msgs = [MagicMock(value={"a": i}) for i in range(3)]
        fake_consumer = MagicMock()
        fake_consumer.__iter__.return_value = iter(msgs)
        fake_gcs = MagicMock()
        with patch("sys.argv", ["prog", "--topic", "t", "--prefix", "p"]), \
             patch("src.consumers.kafka_gcs_sink.KafkaConsumer", return_value=fake_consumer), \
             patch("src.consumers.kafka_gcs_sink.gcs_client", return_value=fake_gcs), \
             patch("src.consumers.kafka_gcs_sink.signal.signal"):
            sink.main()
        # Partial flush of buffer happens at end
        assert fake_gcs.put_object.called
        assert fake_consumer.close.called

    def test_main_flush_at_threshold(self):
        msgs = [MagicMock(value={"i": i}) for i in range(5)]
        fake_consumer = MagicMock()
        fake_consumer.__iter__.return_value = iter(msgs)
        fake_gcs = MagicMock()
        with patch("sys.argv", ["prog", "--topic", "t", "--prefix", "p"]), \
             patch("src.consumers.kafka_gcs_sink.KafkaConsumer", return_value=fake_consumer), \
             patch("src.consumers.kafka_gcs_sink.gcs_client", return_value=fake_gcs), \
             patch("src.consumers.kafka_gcs_sink.FLUSH_SIZE", 2), \
             patch("src.consumers.kafka_gcs_sink.signal.signal"):
            sink.main()
        assert fake_gcs.put_object.call_count >= 2
