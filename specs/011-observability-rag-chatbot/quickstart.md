# Quickstart: Observability Log Persistence & RAG Chatbot

**Feature**: 011-observability-rag-chatbot

## What this feature adds

After every pipeline run, a JSON log file is written to `output/run_logs/` and key metrics are pushed to Prometheus Pushgateway. A new "Observability" mode in the Streamlit wizard lets you ask natural-language questions about past pipeline executions. A Grafana dashboard provides visual analytics over all runs.

## Prerequisites

No new Python dependencies. Uses the existing Poetry environment and `.env` configuration.

For the Grafana dashboard: Docker (to run Prometheus + Pushgateway + Grafana).

## Running the pipeline (logs auto-saved)

```bash
poetry run python demo.py
# Logs written to output/run_logs/ after each run
ls output/run_logs/
# run_20260421T143012_a3f7b2c1.json
# run_20260421T150345_d9e2f801.json
```

## Accessing the chatbot

```bash
poetry run streamlit run app.py
```

In the sidebar, select **Observability** (radio button at top of sidebar). The chatbot loads automatically. Click "Refresh logs" to pick up new runs, then ask questions in the chat input:

- "What was the DQ score delta in the last run?"
- "Which runs triggered Agent 2 code generation?"
- "Were there any quarantined rows in the FDA runs?"
- "Is enrichment hit rate improving over time?"

## Log file location

```
output/run_logs/
└── run_<timestamp>_<uuid8>.json    # one per pipeline run
```

`output/` is gitignored — logs are local only.

## Running tests

```bash
poetry run pytest tests/uc2_observability/ -v
```

## Grafana dashboard

Start the observability stack (Prometheus + Pushgateway + Grafana):

```bash
cd grafana/
docker compose up -d
```

Services:
- Pushgateway: http://localhost:9091 — receives metrics after each pipeline run
- Prometheus: http://localhost:9090 — scrapes Pushgateway every 15s
- Grafana: http://localhost:3000 — default login `admin/admin`

The **Pipeline Observability** dashboard is auto-provisioned. After running the pipeline at least once, open Grafana and select it from the dashboard list.

To stop:
```bash
cd grafana/
docker compose down
```

Metric push is best-effort — if Pushgateway is not running, the pipeline still completes normally and JSON logs are still written.

## Checking log content manually

```bash
python -c "
import json, pathlib
logs = sorted(pathlib.Path('output/run_logs').glob('*.json'))
print(json.dumps(json.loads(logs[-1].read_text()), indent=2))
"
```
