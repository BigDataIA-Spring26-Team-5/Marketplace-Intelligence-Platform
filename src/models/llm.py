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
    """Model string for Agent 1.5 — gap analysis critic.

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
    raw = call_llm(model, messages, temperature)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Try extracting JSON from markdown code block
        if "```json" in raw:
            start = raw.index("```json") + 7
            end = raw.index("```", start)
            return json.loads(raw[start:end].strip())
        if "```" in raw:
            start = raw.index("```") + 3
            end = raw.index("```", start)
            return json.loads(raw[start:end].strip())
        raise
