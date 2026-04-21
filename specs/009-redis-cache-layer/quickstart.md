# Quickstart: Redis Cache Layer

## Prerequisites

### 1. Install Redis (GCP VM)

```bash
sudo dnf install redis -y          # Fedora/RHEL
sudo systemctl enable --now redis
redis-cli ping                     # should return PONG
```

### 2. Add redis-py to project

```bash
cd "/home/aq/work/NEU/SPRING_26/Big Data/ETL"
poetry add redis
```

`msgpack` is NOT required — embedding vectors use `numpy.tobytes()`.

### 3. Configure RDB snapshot persistence

```bash
# Add to /etc/redis/redis.conf (or ~/.redis.conf)
echo "save 3600 1" | sudo tee -a /etc/redis/redis.conf
sudo systemctl restart redis

# Verify persistence is active
redis-cli config get save
# Expected: save → 3600 1
```

### 4. Verify connection

```bash
redis-cli -h localhost -p 6379 info server | head -5
```

---

## Running with cache

```bash
# Normal run — cache active (default)
poetry run python demo.py

# Skip all cache reads/writes (debug/validation)
poetry run python demo.py --no-cache

# Clear all pipeline cache entries before run
poetry run python demo.py --flush-cache

# Both: flush then run without caching
poetry run python demo.py --flush-cache --no-cache
```

---

## Verifying cache behavior

### Check YAML mapping cached after first partition

```bash
redis-cli keys "yaml:*"
# should show one key after first partition runs
```

On a cache hit, `analyze_schema_node` returns immediately without any LLM calls and sets `cache_yaml_hit=True`. If the YAML file was deleted from disk (e.g., between runs on different machines or after a `/tmp` flush), the node **re-materializes it** from the cached `__yaml_text__` payload — writing the full YAML back to `mapping_yaml_path` before the pipeline continues. No LLM call is needed for re-materialization.

```
# Log line confirming YAML cache hit and re-materialization:
INFO  YAML cache: re-materialized /path/to/mapping.yaml
INFO  Cache HIT: loading YAML mapping from Redis (schema fingerprint abc1234567890def)
```

### Check cache hit rate in logs

After a run, look for lines like:

```
INFO  yaml: 12 hits, 1 misses (92.3%)
INFO  llm: 3842 hits, 924 misses (80.6%)
INFO  emb: 2987 hits, 1318 misses (69.4%)
INFO  dedup: 1204 hits, 296 misses (80.3%)
```

### Validate output consistency

```bash
# Run with cache
poetry run python demo.py
cp output/usda_unified.csv /tmp/with_cache.csv

# Run without cache
poetry run python demo.py --no-cache
cp output/usda_unified.csv /tmp/no_cache.csv

# Diff — should be identical (SC-004)
python -c "
import pandas as pd
a = pd.read_csv('/tmp/with_cache.csv')
b = pd.read_csv('/tmp/no_cache.csv')
assert a.equals(b), 'DIFF DETECTED'
print('OK: outputs identical')
"
```

---

## Simulating Redis failure (SC-005 test)

```bash
# Start run, then kill Redis mid-run
poetry run python demo.py &
sleep 5
sudo systemctl stop redis

# Pipeline should complete with warning logs:
# WARNING  Redis unavailable: [connection error]. Cache disabled for this run.
# No crash. Full computation continues.
```

---

## Environment variables for TTL tuning

```bash
# .env or shell — all in seconds
export CACHE_TTL_YAML=2592000    # 30 days (default)
export CACHE_TTL_LLM=604800      # 7 days (default)
export CACHE_TTL_EMB=2592000     # 30 days (default)
export CACHE_TTL_DEDUP=1209600   # 14 days (default)
```

---

## Streamlit app (app.py)

Cache controls appear in the sidebar:
- **Bypass cache** checkbox — equivalent to `--no-cache`
- **Flush cache** button — clears all pipeline keys before next run
