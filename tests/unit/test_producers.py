"""Unit tests for OFF and openFDA producers."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.producers import off_producer as off
from src.producers import openfda_producer as fda


class TestOffProducer:
    def test_gcs_client_calls_boto(self):
        with patch("src.producers.off_producer.boto3.client") as m:
            off.gcs_client()
        assert m.called

    def test_flush_writes_object(self):
        client = MagicMock()
        buffer = [{"code": "1"}, {"code": "2"}]
        off.flush(client, buffer, "2024/01/01", 3)
        args, kwargs = client.put_object.call_args
        assert kwargs["Key"] == "off/2024/01/01/part_0003.jsonl"
        assert kwargs["ContentType"] == "application/x-ndjson"

    def test_main_flushes(self):
        fake_gcs = MagicMock()
        records = [{"product_name": "a", "code": "1"},
                   {"product_name": None},  # skipped
                   {"product_name": "b"}]
        with patch("src.producers.off_producer.gcs_client", return_value=fake_gcs), \
             patch("src.producers.off_producer.load_dataset", return_value=iter(records)), \
             patch("src.producers.off_producer.MAX_RECORDS", 10), \
             patch("src.producers.off_producer.FLUSH_EVERY", 1):
            off.main()
        assert fake_gcs.put_object.called


class TestOpenfdaProducer:
    def test_gcs_client(self):
        with patch("src.producers.openfda_producer.boto3.client") as m:
            fda.gcs_client()
        assert m.called

    def test_fetch_page_success(self):
        resp = MagicMock(status_code=200)
        resp.json.return_value = {"results": [{"x": 1}]}
        with patch("src.producers.openfda_producer.requests.get", return_value=resp):
            assert fda.fetch_page(0) == [{"x": 1}]

    def test_fetch_page_404(self):
        resp = MagicMock(status_code=404)
        with patch("src.producers.openfda_producer.requests.get", return_value=resp):
            assert fda.fetch_page(0) == []

    def test_fetch_page_rate_limit_then_success(self):
        r1 = MagicMock(status_code=429)
        r2 = MagicMock(status_code=200)
        r2.json.return_value = {"results": [{"x": 1}]}
        with patch("src.producers.openfda_producer.requests.get", side_effect=[r1, r2]), \
             patch("src.producers.openfda_producer.time.sleep"):
            assert fda.fetch_page(0) == [{"x": 1}]

    def test_fetch_page_exception_returns_empty(self):
        with patch("src.producers.openfda_producer.requests.get",
                   side_effect=fda.requests.RequestException("x")), \
             patch("src.producers.openfda_producer.time.sleep"):
            assert fda.fetch_page(0) == []

    def test_flush_uploads(self):
        client = MagicMock()
        fda.flush(client, [{"a": 1}], "2024/01/01", 0)
        args, kwargs = client.put_object.call_args
        assert kwargs["Key"] == "openfda/2024/01/01/part_0000.jsonl"

    def test_main_loops_and_exits_on_empty(self):
        fake = MagicMock()
        with patch("src.producers.openfda_producer.gcs_client", return_value=fake), \
             patch("src.producers.openfda_producer.fetch_page", return_value=[]):
            fda.main()
        assert not fake.put_object.called

    def test_main_flushes_partial(self):
        fake = MagicMock()
        pages = [[{"r": 1}] * 5, []]
        it = iter(pages)
        with patch("src.producers.openfda_producer.gcs_client", return_value=fake), \
             patch("src.producers.openfda_producer.fetch_page", side_effect=lambda s: next(it)):
            fda.main()
        assert fake.put_object.called
