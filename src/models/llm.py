"""LiteLLM wrapper for multi-provider model routing."""

import json
import logging

import litellm
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Suppress litellm's verbose logging — our own orchestrator logs are sufficient
litellm.set_verbose = False
litellm.suppress_debug_info = True
for _name in ("LiteLLM", "litellm", "httpx", "httpcore"):
    logging.getLogger(_name).setLevel(logging.WARNING)


def get_orchestrator_llm() -> str:
    """Model string for Agent 1 — schema analysis, gap detection."""
    return "deepseek/deepseek-chat"


def get_codegen_llm() -> str:
    """Model string for Agent 2 — code generation."""
    return "deepseek/deepseek-chat"


def get_enrichment_llm() -> str:
    """Model string for Tier 4 enrichment."""
    return "deepseek/deepseek-chat"


def get_critic_llm() -> str:
    """Model string for Agent 2 — gap analysis critic.

    Uses a reasoning model for higher accuracy on verification tasks.
    Falls back to orchestrator model if reasoning model unavailable.
    """
    try:
        return "deepseek/deepseek-reasoner"
    except Exception:
        logger.warning(
            "deepseek-reasoner unavailable — critic running on non-reasoning model"
        )
        return get_orchestrator_llm()


def call_llm(model: str, messages: list[dict], temperature: float = 0.0) -> str:
    """Unified LLM call through LiteLLM. Returns the assistant message content."""
    response = litellm.completion(
        model=model,
        messages=messages,
        temperature=temperature,
    )
    return response.choices[0].message.content


def call_llm_json(model: str, messages: list[dict], temperature: float = 0.0) -> dict:
    """LLM call that parses response as JSON. Falls back to extracting JSON from markdown."""
    import re
    raw = call_llm(model, messages, temperature)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r'```(?:json)?\s*([\s\S]*?)```', raw)
        if m:
            return json.loads(m.group(1).strip())
        raise


# ── LLM call counter (UC2 observability) ─────────────────────────────

_llm_call_counter: int = 0


def reset_llm_counter() -> None:
    global _llm_call_counter
    _llm_call_counter = 0


def get_llm_call_count() -> int:
    return _llm_call_counter


def get_observability_llm() -> str:
    """Model string for UC2 observability queries."""
    return get_enrichment_llm()


# Patch call_llm to increment counter
_original_call_llm = call_llm


def call_llm(model: str, messages: list[dict], temperature: float = 0.0) -> str:  # type: ignore[misc]
    global _llm_call_counter
    _llm_call_counter += 1
    return _original_call_llm(model, messages, temperature)


# ── UC2 import guard ──────────────────────────────────────────────────

try:
    from src.uc2_observability.kafka_to_pg import emit_event as _emit_event  # type: ignore[import]
    from src.uc2_observability.metrics_collector import MetricsCollector as _MetricsCollector  # type: ignore[import]
    _UC2_AVAILABLE = True
except ImportError:
    _emit_event = None  # type: ignore[assignment]
    _MetricsCollector = None  # type: ignore[assignment]
    _UC2_AVAILABLE = False
