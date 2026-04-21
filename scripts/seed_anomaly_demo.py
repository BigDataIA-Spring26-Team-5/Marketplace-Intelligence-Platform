"""
Seed demo metrics into Prometheus Pushgateway so the anomaly detector
has enough data to run AND flags one obvious outlier.

Pushes 6 synthetic OFF runs:
  - Runs 1-5: normal metrics (dq_score ~0.85, null_rate ~0.03)
  - Run 6:    anomalous (null_rate spikes to 0.72, dq_score drops to 0.41)

After running this, the anomaly detector will flag run 6 as an outlier
and Grafana's Anomaly Flag panel will show a red marker.
"""
import sys
import os
sys.path.insert(0, "/home/bhavyalikhitha_bbl/bhavya-workspace")

from src.uc2_observability.metrics_collector import MetricsCollector

# 5 normal runs
normal_runs = [
    {
        "rows_in":                 95000,
        "rows_out":                93100,
        "dq_score_pre":            0.71,
        "dq_score_post":           0.86,
        "dq_delta":                0.15,
        "null_rate":               0.03,
        "dedup_rate":              0.02,
        "s1_count":                51000,
        "s2_count":                28000,
        "s3_count":                10000,
        "s4_count":                4100,
        "cost_usd":                1.24,
        "llm_calls":               4100,
        "quarantine_rows":         12,
        "block_duration_seconds":  312.4,
    },
    {
        "rows_in":                 96200,
        "rows_out":                94400,
        "dq_score_pre":            0.72,
        "dq_score_post":           0.87,
        "dq_delta":                0.15,
        "null_rate":               0.028,
        "dedup_rate":              0.019,
        "s1_count":                52000,
        "s2_count":                28500,
        "s3_count":                9800,
        "s4_count":                4100,
        "cost_usd":                1.22,
        "llm_calls":               4100,
        "quarantine_rows":         9,
        "block_duration_seconds":  308.1,
    },
    {
        "rows_in":                 94800,
        "rows_out":                93000,
        "dq_score_pre":            0.70,
        "dq_score_post":           0.85,
        "dq_delta":                0.15,
        "null_rate":               0.031,
        "dedup_rate":              0.021,
        "s1_count":                50500,
        "s2_count":                27800,
        "s3_count":                10200,
        "s4_count":                4500,
        "cost_usd":                1.31,
        "llm_calls":               4500,
        "quarantine_rows":         14,
        "block_duration_seconds":  319.7,
    },
    {
        "rows_in":                 97100,
        "rows_out":                95200,
        "dq_score_pre":            0.73,
        "dq_score_post":           0.88,
        "dq_delta":                0.15,
        "null_rate":               0.027,
        "dedup_rate":              0.018,
        "s1_count":                53000,
        "s2_count":                29000,
        "s3_count":                9500,
        "s4_count":                3700,
        "cost_usd":                1.18,
        "llm_calls":               3700,
        "quarantine_rows":         8,
        "block_duration_seconds":  301.5,
    },
    {
        "rows_in":                 95500,
        "rows_out":                93700,
        "dq_score_pre":            0.71,
        "dq_score_post":           0.86,
        "dq_delta":                0.15,
        "null_rate":               0.029,
        "dedup_rate":              0.020,
        "s1_count":                51500,
        "s2_count":                28200,
        "s3_count":                9900,
        "s4_count":                4100,
        "cost_usd":                1.25,
        "llm_calls":               4100,
        "quarantine_rows":         11,
        "block_duration_seconds":  310.2,
    },
]

# The anomalous run — brand_name shipped as literal "NULL" strings
anomalous_run = {
    "rows_in":                 95800,
    "rows_out":                48900,   # massive drop — 47% rows quarantined
    "dq_score_pre":            0.69,
    "dq_score_post":           0.41,    # DQ crashed
    "dq_delta":                -0.28,   # negative delta — got WORSE
    "null_rate":               0.72,    # 72% null rate — spike from ~3%
    "dedup_rate":              0.49,    # dedup rate exploded
    "s1_count":                22000,
    "s2_count":                15000,
    "s3_count":                8000,
    "s4_count":                3900,
    "cost_usd":                3.81,    # cost spiked — S4 fallback overused
    "llm_calls":               9800,
    "quarantine_rows":         46900,   # 47k rows quarantined
    "block_duration_seconds":  891.3,   # 3x slower
}


def main():
    print("Seeding 5 normal OFF runs into Prometheus Pushgateway...")
    for i, metrics in enumerate(normal_runs, 1):
        run_id = f"OFF_seed_run_{i:02d}"
        MetricsCollector().push(run_id=run_id, source="OFF", metrics_dict=metrics)
        print(f"  Pushed: {run_id} (null_rate={metrics['null_rate']}, dq_post={metrics['dq_score_post']})")

    print("\nPushing ANOMALOUS run (OFF_seed_run_06)...")
    MetricsCollector().push(run_id="OFF_seed_run_06", source="OFF", metrics_dict=anomalous_run)
    print(f"  Pushed: OFF_seed_run_06 (null_rate={anomalous_run['null_rate']}, dq_post={anomalous_run['dq_score_post']})")

    print("\nAll done. Now running anomaly detector...")
    from src.uc2_observability.anomaly_detector import AnomalyDetector
    detector = AnomalyDetector()
    for source in ["OFF", "USDA", "openFDA", "ESCI"]:
        reports = detector.run_detection(source, n_runs=20)
        status = f"{len(reports)} ANOMALY(IES)" if reports else "normal"
        print(f"  {source}: {status}")

    print("\nCheck Grafana at http://35.239.47.242:3000 — Anomaly Flag panel should show a red marker.")
    print("Ask the chatbot: 'Why was OFF run 6 flagged as an anomaly?'")


if __name__ == "__main__":
    main()
