# Quickstart: UC1 → UC2 Observability Integration

**Feature**: 010-uc1-uc2-integration

## Prerequisites

1. Pull the team's shared branch containing `src/uc2_observability/metrics_collector.py` and `src/uc2_observability/kafka_to_pg.py`
2. UC2 services running on GCP VM: Kafka (`localhost:9092`), Prometheus Pushgateway (`localhost:9091`), Postgres (`localhost:5432`)
3. `kafka_to_pg` consumer started: `nohup python3 -m src.uc2_observability.kafka_to_pg &`

## Running UC1 with UC2 emission

```bash
# Standard pipeline run — UC2 events emit automatically
poetry run python demo.py

# Or via Streamlit wizard
poetry run streamlit run app.py
```

## Verifying block trace in Postgres

```sql
SELECT block, rows_in, rows_out, duration_ms
FROM block_trace
WHERE run_id = '<run_id_from_logs>'
ORDER BY ts;
```

## Verifying run lifecycle in Postgres

```sql
SELECT event_type, source, run_id, status
FROM audit_events
ORDER BY ts DESC LIMIT 10;
```

## Verifying Prometheus metrics

Open `http://35.239.47.242:9091` in browser — look for job=`uc1_pipeline`.

Or via PromQL:
```
uc1_dq_score_post{source="usda_fooddata_sample"}
```

## Verifying Grafana

Open `http://35.239.47.242:3000/d/uc1-pipeline` — all 8 panels should show data after one run.

## Running without UC2 (local dev)

If UC2 modules aren't available or GCP is unreachable, the import guard (`_UC2_AVAILABLE = False`) ensures the pipeline runs normally with no errors — only a warning log appears.

## Running tests

```bash
poetry run pytest tests/uc2_observability/ -v
```
