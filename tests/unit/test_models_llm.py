"""Unit tests for src.models.llm wrapper."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.models import llm


class TestGetters:
    def test_orchestrator(self):
        assert isinstance(llm.get_orchestrator_llm(), str)

    def test_codegen(self):
        assert isinstance(llm.get_codegen_llm(), str)

    def test_enrichment(self):
        assert isinstance(llm.get_enrichment_llm(), str)

    def test_critic(self):
        assert isinstance(llm.get_critic_llm(), str)

    def test_observability(self):
        assert isinstance(llm.get_observability_llm(), str)


class TestInferProvider:
    def test_deepseek(self):
        assert llm._infer_provider("deepseek/deepseek-chat") == "deepseek"

    def test_groq(self):
        assert llm._infer_provider("groq/llama-3") == "groq"

    def test_anthropic_default(self):
        assert llm._infer_provider("claude-sonnet-4") == "anthropic"


class TestRateConfig:
    def test_load_config(self):
        llm._rate_limits_cache = {}
        cfg = llm._load_rate_config("anthropic")
        assert isinstance(cfg, dict)

    def test_unknown_provider_fallback(self):
        llm._rate_limits_cache = {"anthropic": {"x": 1}}
        cfg = llm._load_rate_config("unknown")
        assert cfg == {"x": 1}


class TestCallLlm:
    def test_call_llm_success(self):
        fake_resp = MagicMock()
        fake_resp.choices = [MagicMock(message=MagicMock(content="hello"))]
        with patch.object(llm.litellm, "completion", return_value=fake_resp):
            result = llm.call_llm("claude-x", [{"role": "user", "content": "hi"}])
        assert result == "hello"

    def test_call_llm_counter_increments(self):
        llm.reset_llm_counter()
        fake_resp = MagicMock()
        fake_resp.choices = [MagicMock(message=MagicMock(content="ok"))]
        with patch.object(llm.litellm, "completion", return_value=fake_resp):
            llm.call_llm("m", [])
            llm.call_llm("m", [])
        assert llm.get_llm_call_count() == 2

    def test_rate_limit_retry(self):
        fake_resp = MagicMock()
        fake_resp.choices = [MagicMock(message=MagicMock(content="ok"))]
        err = llm.litellm.exceptions.RateLimitError(
            message="rl", model="m", llm_provider="anthropic"
        )
        with patch.object(llm.litellm, "completion", side_effect=[err, fake_resp]), \
             patch.object(llm.time, "sleep"):
            result = llm.call_llm("m", [])
        assert result == "ok"

    def test_rate_limit_exhaust_raises(self):
        err = llm.litellm.exceptions.RateLimitError(
            message="rl", model="m", llm_provider="anthropic"
        )
        llm._rate_limits_cache = {"anthropic": {
            "retry_max_attempts": 2, "retry_base_delay_seconds": 0,
            "retry_jitter_fraction": 0,
        }}
        with patch.object(llm.litellm, "completion", side_effect=err), \
             patch.object(llm.time, "sleep"):
            with pytest.raises(Exception):
                llm.call_llm("m", [])


class TestCallLlmJson:
    def test_plain_json(self):
        with patch.object(llm, "_original_call_llm", return_value='{"a": 1}'):
            out = llm.call_llm_json("m", [])
        assert out == {"a": 1}

    def test_markdown_fenced_json(self):
        raw = "some text\n```json\n{\"x\": 2}\n```"
        with patch.object(llm, "_original_call_llm", return_value=raw):
            out = llm.call_llm_json("m", [])
        assert out == {"x": 2}

    def test_invalid_json_raises(self):
        with patch.object(llm, "_original_call_llm", return_value="not json"):
            with pytest.raises(Exception):
                llm.call_llm_json("m", [])


class TestCounter:
    def test_reset(self):
        llm._llm_call_counter = 5
        llm.reset_llm_counter()
        assert llm.get_llm_call_count() == 0
