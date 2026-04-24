"""ObservabilityChatbot: RAG chatbot for pipeline run history queries."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field

from src.uc2_observability.log_store import RunLogStore

logger = logging.getLogger(__name__)

_UUID_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE
)

_RECENCY_PATTERN = re.compile(r"last\s+(\d+)\s+runs?", re.IGNORECASE)

_METRIC_KEYWORDS = frozenset(
    ["dq", "score", "enrichment", "quarantine", "block", "duration", "rows", "delta"]
)
_TIME_KEYWORDS = frozenset(["last", "recent", "latest", "newest"])

_SYSTEM_PROMPT = (
    "You are a pipeline observability assistant. "
    "Answer questions about pipeline execution history using ONLY the provided run log data. "
    "Cite run_ids for every claim you make. "
    "If multiple runs are provided, compare them chronologically and highlight trends. "
    "If the answer cannot be determined from the logs, say so explicitly."
)


@dataclass
class ChatResponse:
    answer: str
    cited_run_ids: list[str] = field(default_factory=list)
    context_run_count: int = 0


class ObservabilityChatbot:
    def __init__(self, log_store: RunLogStore):
        self._store = log_store
        self._logs: list[dict] = []

    def ingest_audit_logs(self) -> int:
        self._logs = self._store.load_all()
        return len(self._logs)

    def get_relevant_context(self, query: str, max_runs: int = 10) -> list[dict]:
        if not self._logs:
            self._logs = self._store.load_all()

        q_lower = query.lower()

        # Branch 1: specific run_id mentioned
        uuid_matches = _UUID_PATTERN.findall(query)
        if uuid_matches:
            results = [r for r in self._logs if r.get("run_id") in uuid_matches]
            if results:
                return results

        # Branch 2a: multi-source comparison ("compare" + multiple sources mentioned)
        sources = {r.get("source_name") for r in self._logs if r.get("source_name")}
        if "compare" in q_lower and len(sources) > 1:
            mentioned = [s for s in sources if s and s.lower() in q_lower]
            if len(mentioned) >= 2:
                results: list[dict] = []
                sorted_by_src: dict[str, list[dict]] = {}
                for r in self._logs:
                    src = r.get("source_name", "")
                    sorted_by_src.setdefault(src, []).append(r)
                for src_logs in sorted_by_src.values():
                    src_logs.sort(key=lambda r: r.get("timestamp", ""), reverse=True)
                    results.extend(src_logs[:max_runs])
                return results

        # Branch 2b: single source name mentioned
        for src in sources:
            if src and src.lower() in q_lower:
                matched = [r for r in self._logs if r.get("source_name") == src]
                matched.sort(key=lambda r: r.get("timestamp", ""), reverse=True)
                return matched[:max_runs]

        # Branch 3: "last N runs" integer extraction
        recency_match = _RECENCY_PATTERN.search(query)
        if recency_match:
            n = int(recency_match.group(1))
            sorted_logs = sorted(self._logs, key=lambda r: r.get("timestamp", ""), reverse=True)
            return sorted_logs[:n]

        # Branch 3b: recency time words without N
        if any(kw in q_lower for kw in _TIME_KEYWORDS):
            sorted_logs = sorted(self._logs, key=lambda r: r.get("timestamp", ""), reverse=True)
            return sorted_logs[:max_runs]

        # Branch 4: metric keyword
        if any(kw in q_lower for kw in _METRIC_KEYWORDS):
            sorted_logs = sorted(self._logs, key=lambda r: r.get("timestamp", ""), reverse=True)
            return sorted_logs[:max_runs]

        # Default: last max_runs runs
        sorted_logs = sorted(self._logs, key=lambda r: r.get("timestamp", ""), reverse=True)
        return sorted_logs[:max_runs]

    def query(self, question: str) -> ChatResponse:
        """Answer question grounded in run log data. Never raises."""
        try:
            from src.agents.safety_guardrails import get_safety_guardrails
            guardrails = get_safety_guardrails()

            input_check = guardrails.validate_input(question)
            if not input_check.is_safe:
                return ChatResponse(
                    answer=f"Request blocked by input guardrail: {input_check.reason}",
                    cited_run_ids=[],
                    context_run_count=0,
                )

            if not self._logs:
                self._logs = self._store.load_all()

            if not self._logs:
                return ChatResponse(
                    answer="No pipeline run data available. Run the pipeline at least once to generate logs.",
                    cited_run_ids=[],
                    context_run_count=0,
                )

            context = self.get_relevant_context(question)
            context_ids = {r.get("run_id") for r in context if r.get("run_id")}

            from src.models.llm import call_llm, get_observability_llm
            model = get_observability_llm()

            user_content = (
                f"Run logs (JSON):\n{json.dumps(context, indent=2, default=str)}\n\n"
                f"Question: {question}"
            )
            raw_answer = call_llm(
                model=model,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": user_content},
                ],
            )

            output_check = guardrails.validate_output(raw_answer)
            final_answer = output_check.sanitized_text

            cited = [rid for rid in _UUID_PATTERN.findall(final_answer) if rid in context_ids]
            cited = list(dict.fromkeys(cited))  # deduplicate preserving order

            return ChatResponse(
                answer=final_answer,
                cited_run_ids=cited,
                context_run_count=len(context),
            )
        except Exception as exc:
            logger.warning(f"ObservabilityChatbot.query failed: {exc}")
            return ChatResponse(
                answer=f"Unable to answer: {exc}",
                cited_run_ids=[],
                context_run_count=0,
            )
