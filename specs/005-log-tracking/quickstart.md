# Quickstart: Pipeline Run Log Tracking & Observability Chatbot

**Feature**: 005-log-tracking  
**Date**: 2026-04-20  
**Prerequisite**: `ANTHROPIC_API_KEY` set in environment; at least one pipeline run completed.

---

## 0. Set run_type Before Running the Pipeline

```bash
# Mark runs as demo (excluded from dev noise)
export PIPELINE_RUN_TYPE=demo

# Or set in .env
echo "PIPELINE_RUN_TYPE=demo" >> .env
```

Options: `dev` (default) | `demo` | `prod`

---

## 1. Run the Pipeline

```bash
poetry run python demo.py
# or
poetry run streamlit run app.py
```

After the run completes, two artifacts are written automatically:

```
output/logs/{run_id}.json     ← structured JSON sidecar
.chroma/pipeline_audit/       ← ChromaDB document (auto-created)
```

---

## 2. Verify the Log Was Written

```bash
ls output/logs/
# f47ac10b-58cc-4372-a567-0e02b2c3d479.json

python -c "
import chromadb
c = chromadb.PersistentClient('.chroma')
col = c.get_collection('pipeline_audit')
print(f'Documents in store: {col.count()}')
"
```

---

## 3. Query the Chatbot (CLI)

```bash
python -c "
from src.uc2_observability.rag_chatbot import ObservabilityChatbot
chatbot = ObservabilityChatbot()
print(chatbot.query('which run had the highest DQ score improvement?'))
"
```

Example response:
```
Run f47ac10b improved DQ score the most: pre=0.68 → post=0.84 (+0.16).
Source: run_id f47ac10b-58cc-4372-a567-0e02b2c3d479, domain nutrition, completed 2026-04-20.
```

---

## 4. Include Dev Runs in Query

By default, `run_type=dev` runs are excluded.

```python
chatbot.query("why did the last run fail", include_dev=True)
```

---

## 5. Backfill Existing Runs

If you have JSON sidecars from before ChromaDB was set up:

```python
from src.uc2_observability.rag_chatbot import ObservabilityChatbot
chatbot = ObservabilityChatbot()
count = chatbot.ingest_audit_logs("output/logs")
print(f"Ingested {count} run logs")
```

Already-ingested runs are skipped (idempotent).

---

## 6. View the Dashboard

```bash
poetry run streamlit run app.py
```

Navigate to the **Observability** tab. Panels:
- **Run History**: table of all runs with DQ scores, row counts, status
- **DQ Distribution**: pre vs post DQ score comparison per run
- **Block Trace**: duration heatmap across blocks and runs
- **LLM Cost**: calls per run + estimated cost
- **Chatbot**: text input — type any question, get a cited answer

---

## Example Chatbot Questions

```
"Which run quarantined the most rows?"
"What was the average dq_score_post across all demo runs?"
"Which block took the longest in the last run?"
"How many LLM calls did the nutrition domain use?"
"Were there any failed runs this week?"
"What were the most common quarantine reasons?"
```

---

## Troubleshooting

| Symptom | Check |
|---------|-------|
| `output/logs/` is empty | Pipeline may have run before T008 hook was added; re-run the pipeline |
| `No pipeline runs found` from chatbot | ChromaDB collection empty — run `ingest_audit_logs()` manually (step 5) |
| `ANTHROPIC_API_KEY` error | Set `export ANTHROPIC_API_KEY=sk-ant-...` before running chatbot |
| Dev runs appearing in chatbot answers | Pass `include_dev=False` (default); check `PIPELINE_RUN_TYPE` was set correctly at run time |
| ChromaDB `.chroma/` directory not created | First `write_run_log()` call creates it automatically; ensure write permissions in repo root |
