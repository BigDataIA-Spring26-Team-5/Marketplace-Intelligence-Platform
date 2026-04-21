# Interface Contracts: Observability Log Persistence & RAG Chatbot

**Feature**: 011-observability-rag-chatbot  
**Date**: 2026-04-21

These contracts define the boundaries between the observability layer and the rest of the pipeline. They are **internal Python interfaces** — not HTTP APIs.

---

## Contract 1: RunLogWriter.save()

**Producer**: `src/agents/graph.py` (`save_output_node`)  
**Consumer**: `src/uc2_observability/log_writer.py`

```python
def save(
    state: PipelineState,
    status: Literal["success", "partial", "failed"],
    error: str | None = None,
    start_time: float | None = None,  # time.monotonic() value from run start
) -> Path | None:
    ...
```

**Pre-conditions**:
- `state` is the final `PipelineState` dict after all nodes have run (or after exception)
- `status` is one of `"success"`, `"partial"`, `"failed"`
- `error` is non-None when `status != "success"`

**Post-conditions**:
- If write succeeds: returns absolute `Path` to written `.json` file
- If write fails for any reason: returns `None`, logs a warning — **never raises**
- Written file is complete and valid JSON (atomic write via temp+rename)
- Written file contains at minimum: `run_id`, `timestamp`, `source_path`, `status`

**Invariants**:
- Calling `save()` does NOT modify `state`
- Calling `save()` does NOT affect `output/` CSV files
- `run_id` in written file is globally unique (UUID4)

---

## Contract 2: RunLogStore query interface

**Producer**: `src/uc2_observability/log_writer.py` (writes files)  
**Consumer**: `src/uc2_observability/rag_chatbot.py`, Streamlit observability page

```python
def load_all(self) -> list[dict]:
    """All run logs sorted by timestamp ASC. Empty list if no logs exist."""

def get_by_run_id(self, run_id: str) -> dict | None:
    """Exact match on run_id field. None if not found."""

def filter(
    self,
    source_name: str | None = None,
    status: str | None = None,
    since: datetime | None = None,
    limit: int | None = None,
) -> list[dict]:
    """
    Filters applied as AND conditions. Results sorted timestamp DESC.
    limit applies after filtering.
    Empty list (not exception) when no matches.
    """

def summary_stats(self) -> dict:
    """
    Returns:
    {
        "total_runs": int,
        "success_count": int,
        "partial_count": int,
        "failed_count": int,
        "avg_dq_delta": float | None,   # None if no runs have dq_delta
        "avg_duration_seconds": float | None,
        "sources_seen": list[str],       # unique source_name values
    }
    """
```

**Invariants**:
- All methods are read-only — no method modifies files on disk
- Corrupt or unreadable log files are skipped with a logged warning; they do not raise
- `load_all()` result is a stable snapshot; calling it twice may return different results if new runs completed between calls

---

## Contract 3: ObservabilityChatbot.query()

**Producer**: Streamlit observability page  
**Consumer**: `src/uc2_observability/rag_chatbot.py`

```python
@dataclass
class ChatResponse:
    answer: str           # Natural language answer to the question
    cited_run_ids: list[str]  # run_id values referenced in the answer (may be empty)
    context_run_count: int    # How many run logs were included in context

def query(self, question: str) -> ChatResponse:
    ...
```

**Pre-conditions**:
- `question` is a non-empty string
- Chatbot has been initialized (log store accessible)

**Post-conditions**:
- Always returns a `ChatResponse` — never raises
- If no run logs exist: `answer` explains no data available, `cited_run_ids = []`
- If LLM call fails: `answer` explains the error, `cited_run_ids = []`
- If question is out of scope: `answer` states chatbot is scoped to pipeline observability

**Invariants**:
- `query()` is idempotent — same question with same log data returns equivalent answer
- `query()` does NOT modify any run logs
- `cited_run_ids` contains only run IDs that exist in the logs provided as context

---

## Contract 4: graph.py integration point

**Where**: `save_output_node` in `src/agents/graph.py`

```python
# At the start of save_output_node, record start time
_run_start = getattr(state, "_run_start_time", None)  # set at load_source_node

# At the end of save_output_node (success path):
from src.uc2_observability.log_writer import RunLogWriter
RunLogWriter().save(state, status="success", start_time=_run_start)

# In exception handler of save_output_node (failure path):
RunLogWriter().save(state, status="partial", error=str(e), start_time=_run_start)
```

**Invariants**:
- `RunLogWriter().save()` call is wrapped in its own try/except — if it raises, the exception is logged as a warning and not re-raised
- Log write happens AFTER the output CSV is written — the CSV is the primary output
- Log write does NOT gate the pipeline's return value — `save_output_node` returns normally regardless of log write outcome
