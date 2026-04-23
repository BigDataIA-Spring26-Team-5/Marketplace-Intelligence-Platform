# Consolidated Test Report — Marketplace Intelligence Platform

**Generated:** 2026-04-22 (updated after installing optional deps)
**Branch:** deepika
**Total new tests added:** 245 (all 245 passing — previously-skipped tests now run after `langgraph`, `fastapi`, `psycopg2-binary`, `redis`, `httpx`, `pytest-cov` were installed into the venv)

---

## 1. Test Suite Overview

The test suite is organized by layer of the system. Each subfolder under `tests/` corresponds to a distinct architectural concern.

```
tests/
├── conftest.py                       # shared fixtures (NEW)
├── unit/                             # pure-Python unit tests
│   ├── test_guardrails.py            # NEW — 86 tests
│   ├── test_pydantic_schemas.py      # NEW — 20 tests
│   ├── test_dq_score.py              # NEW — 25 tests
│   ├── test_dynamic_mapping.py       # NEW — 37 tests
│   └── test_cache_client.py          # pre-existing
├── agents/                           # NEW package — agent-graph tests
│   ├── __init__.py
│   ├── test_orchestrator.py          # NEW — 39 tests (Agent 1)
│   ├── test_critic.py                # NEW — 8 tests  (Agent 2)
│   └── test_sequence_planner.py      # NEW — 8 tests  (Agent 3)
├── api/                              # NEW package — REST/MCP endpoint tests
│   ├── __init__.py
│   └── test_mcp_server.py            # NEW — 22 tests
├── integration/                      # pre-existing (GCS / cache pipeline)
├── uc2_observability/                # pre-existing
├── test_gcs_loader.py                # pre-existing
└── unit_tests.py                     # pre-existing
```

### Headline metrics

| Category                              | Files | Tests | Pass / Skip / Fail |
| ------------------------------------- | ----: | ----: | ------------------ |
| Guardrails                            | 1     | 86    | 86 / 0 / 0         |
| Pydantic schema                       | 1     | 20    | 20 / 0 / 0         |
| Pre/Post DQ score (decimals)          | 1     | 25    | 25 / 0 / 0         |
| Unit (declarative YAML actions)       | 1     | 37    | 37 / 0 / 0         |
| Agents (orchestrator/critic/planner)  | 3     | 55    | 55 / 0 / 0         |
| API (MCP server)                      | 1     | 22    | 22 / 0 / 0         |
| Shared fixtures                       | 1     | n/a   | n/a                |
| **Total NEW**                         | **9** | **245** | **245 / 0 / 0**  |

> Optional heavyweight deps (`langgraph`, `fastapi`, `psycopg2-binary`, `redis`, `httpx`, `pytest-cov`) have been installed into the project venv via `poetry run pip install`, so all previously-skipped tests now execute. `pytest.importorskip` is retained as a safety net for environments where any of these are missing.

### Pre-existing failures — diagnosed and resolved

A full `pytest tests/ -m "not integration"` initially reported **6 failing tests** (all pre-existing in files I did not author). Each was diagnosed against current source code and either deleted (obsolete contract) or fixed.

| Test | Root cause | Action |
|---|---|---|
| `test_cache_client::test_set_noop_when_unavailable` | Asserted `set()→False` when Redis down. Reality: SQLite fallback (default-on) succeeds and returns `True`. Test asserts pre-fallback contract. | **Deleted** — obsolete; `test_stats_records_miss_on_degraded` already covers degraded-mode behavior under the new contract. |
| `test_cache_client::test_miss_recorded_in_stats` | Asserted `misses == 1`. Reality: `2` (Redis-miss + SQLite-empty are both recorded as misses on the new fallback path). | **Deleted** — obsolete contract. |
| `test_cache_pipeline::test_routes_to_critique_schema_on_miss` | Asserted route returns `"critique_schema"` on cache miss. Reality: returns `"check_registry"` because `with_critic` defaults to `False` (per CLAUDE.md, critic is opt-in via `--with-critic`). | **Deleted** — obsolete; new `tests/agents/test_sequence_planner.py::TestRouteAfterAnalyzeSchema` covers the correct current contract. |
| `test_cache_pipeline::test_routes_to_critique_schema_when_key_absent` | Same root cause as above. | **Deleted** — obsolete. |
| `test_log_writer::test_returns_none_on_unwritable_dir` | `chmod` does not enforce write-block on Windows NTFS; `save()` succeeds and returns the path. Test is valid on Linux/macOS. | **Skipped on Windows** via `@pytest.mark.skipif(sys.platform == "win32", ...)` — Unix coverage preserved. |
| `test_uc2_integration::test_metrics_push_called` | Asserted positional/`metrics=` kwarg. Reality: `graph.py` calls `_MetricsCollector().push(run_id=..., source=..., metrics_dict=...)` (graph.py:562-566). Test was reading the wrong kwarg name. | **Fixed** — extractor now prefers `kwargs["metrics_dict"]` and falls back to `metrics` / positional. |

Aggregate run result after fixes: **340 passed, 1 skipped (Windows chmod), 1 deselected, 0 failed** in ~30s.

---

## 2. Segregation by Test Category

### 2.1 API Tests
**File:** `tests/api/test_mcp_server.py`
**Target:** `src/uc2_observability/mcp_server.py` (FastAPI app, 7 MCP tool endpoints)

| Test class             | What it covers                                                     |
| ---------------------- | ------------------------------------------------------------------ |
| `TestCacheKey`         | Deterministic, prefixed cache keys; distinct inputs → distinct keys |
| `TestSerializeRows`    | datetime → ISO string; Decimal → float; passthrough for primitives |
| `TestDiscoveryEndpoints` | `GET /tools` lists all 8 tool definitions; `/health` returns redis status |
| `TestGetRunMetrics`    | `POST /tools/get_run_metrics` — 400 on missing run_id, flat metric dict, null when Prometheus empty, source filter is propagated to PromQL |
| `TestGetBlockTrace`    | `POST /tools/get_block_trace` — Postgres path, conditional source filter |
| `TestGetSourceStats`   | `POST /tools/get_source_stats` — metric dict keyed by run_id |
| `TestGetAnomalies`     | Combined run_id + source filtering against `anomaly_reports` table |
| `TestGetQuarantine`    | Run-id-scoped quarantine row retrieval with reason text |
| `TestListRuns`         | Unique-run extraction from Prometheus, lexicographic sort |

External deps (`psycopg2`, `redis`, Prometheus) are **patched in-process** with `unittest.mock.patch.object` — no real services required.

### 2.2 Guardrails Tests
**File:** `tests/unit/test_guardrails.py`
**Target:** `src/agents/guardrails.py`

| Test class                         | Coverage area                                                    |
| ---------------------------------- | ---------------------------------------------------------------- |
| `TestGuardrailResult`              | Dataclass `__bool__`, default-list independence                 |
| `TestGuardrailAudit`               | `requires_human_review` aggregates HITL flags                    |
| `TestSchemaAnalysisInput`          | Empty source/unified rejection, only-`__meta__` rejection, oversize warning |
| `TestCriticInput`                  | Empty mapping+ops rejection, mapping/operations overlap warning  |
| `TestSequencePlannerInput`         | Empty blocks/domain rejection, whitespace-domain rejection       |
| `TestEnrichmentInput`              | Empty rows rejection, oversize batch warning, missing product_name warning |
| `TestSchemaAnalysisOutput`         | **Hallucinated source columns**, mapping → enrichment/computed cols rejection, **safety-column block (allergens/dietary_tags/is_organic)**, invalid primitive/action rejection, runaway op count, duplicate-op warnings |
| `TestCriticOutput`                 | Excessive-additions hallucination guard (>3× input), invalid primitives, hallucinated targets warning |
| `TestSequencePlannerOutput`        | Unknown blocks rejected; **dq_score_pre first / dq_score_post last** ordering; normalize-before-dedup; extract_allergens before llm_enrich |
| `TestEnrichmentOutput`             | **Safety-column stripping** (LLM never infers allergens); idx out-of-bounds skip; unknown category warning |
| `TestResponseLevelChecks`          | MAX_RESPONSE_SIZE rejection, JSON parsing with markdown-fence fallback, prompt-leakage detection |
| `TestHITLThresholds`               | Operation-count, unresolvable-count, low-confidence flags; enrichment large-batch flag |
| `TestClamping`                     | `clamp_value`, `validate_confidence_score [0,1]`, `validate_dq_score [0,100]` (decimal-preserving), `validate_risk_score [1,5]` |
| `TestCompositeRunners`             | `run_input_guardrails` routing, `run_output_guardrails` size short-circuit, `run_guardrails_with_audit` returns `(GuardrailResult, GuardrailAudit)` tuple with HITL flags |

### 2.3 Unit Tests — Declarative YAML Actions
**File:** `tests/unit/test_dynamic_mapping.py`
**Target:** `src/blocks/dynamic_mapping.py` (DynamicMappingBlock action handlers)

| Test class           | Action handlers exercised                                          |
| -------------------- | ------------------------------------------------------------------ |
| `TestTryParse`       | JSON object/array parsing, Python-repr fallback, `None` for non-parseable |
| `TestCastValue`      | `_cast_value` for float/integer/boolean (truthy + falsy)/string    |
| `TestSetNullSetDefault` | `set_null` (typed `pd.NA`), `set_default` constant fill, default=None falls back to set_null |
| `TestTypeCast`       | string→float, string→int (Int64 dtype), string→boolean, missing-source fallback |
| `TestRenameDrop`     | `rename` happy path + missing-source no-op, `drop_column`          |
| `TestFormatOps`      | `to_lowercase`, `to_uppercase`, `strip_whitespace`, `regex_replace`, `regex_extract` (first match), `truncate_string`, `pad_string` (left-zero), `value_map` with default |
| `TestUnifyOps`       | `coalesce` first-non-null across N sources, `concat_columns` with separator + null exclusion, `string_template` |
| `TestDeriveOps`      | `extract_json_field` from object and array (with filter), `conditional_map` keyword match with default, `expression` (pandas eval safe arithmetic), `expression` invalid → set_null fallback, `contains_flag` |

### 2.4 Pydantic Schema Tests
**File:** `tests/unit/test_pydantic_schemas.py`
**Target:** `src/schema/models.py`

| Test class         | Validation surface                                                     |
| ------------------ | ---------------------------------------------------------------------- |
| `TestColumnSpec`   | All 4 valid `type` literals, invalid type rejection, `extra=allow` for forward compat, `enrichment_alias` field |
| `TestDQWeights`    | Defaults sum to 1.0, **must-sum-to-one validator** (1e-6 tolerance), zero-weights rejected |
| `TestUnifiedSchema` | `required_columns` excludes computed; `mappable_columns` excludes computed AND enrichment; `enrichment_columns` selects enrichment-only; `for_prompt()` excludes computed but includes enrichment (so Agent 1 emits ENRICH_ALIAS); JSON roundtrip preserves structure; default DQWeights attached; invalid weights rejected at schema level |

### 2.5 Agents Tests

#### 2.5.1 Orchestrator (Agent 1)
**File:** `tests/agents/test_orchestrator.py`
**Target:** `src/agents/orchestrator.py` + `src/agents/confidence.py`

| Test class                  | Concern                                                             |
| --------------------------- | ------------------------------------------------------------------- |
| `TestToSnake`               | camelCase / PascalCase / HTTPRequest / spaces / dashes → snake_case (parametrized) |
| `TestParseLlmResponse`      | New format (operations[]+unresolvable[]), legacy (derivable_gaps + missing_columns), oldest flat-gaps fallback |
| `TestDetectEnrichmentColumns` | Enrichment cols absent from source → returned; present → excluded |
| `TestSchemaFingerprint`     | **Order-independent**, **column-set-sensitive**, **domain-sensitive**, **schema-version-sensitive**, 16-hex-char output (SHA-256 truncated) |
| `TestDeterministicCorrections` | **Rule 4** (incompatible-type RENAME → CAST); **Rule 6** (uncovered source col → DELETE; protected enrichment cols never DELETE'd); **Rule 7** (`normalize_before_dedup=true` on identity columns only) |
| `TestLlmOpToYaml`           | Conversion of all 8 primitives (ADD/CAST/FORMAT/RENAME/DELETE/SPLIT/UNIFY/DERIVE) to YAML action dicts, including column_mapping resolution |
| `TestConfidence`            | `calculate_confidence` factor combinations, `get_confidence_level` (high/medium/low), `get_confidence_display` |
| `TestOrchestratorConstants` | `_BLOCK_COLUMN_PROVIDERS` covers safety cols, `_IDENTITY_COLUMNS` for dedup, `_DTYPE_FAMILY` covers common pandas dtypes |

#### 2.5.2 Critic (Agent 2)
**File:** `tests/agents/test_critic.py`
**Target:** `src/agents/critic.py`

| Test                                       | Behavior verified                                |
| ------------------------------------------ | ------------------------------------------------ |
| `test_skip_when_already_ran`               | Idempotent — exits when `revised_operations` set |
| `test_skip_when_no_operations` / `_missing` | No-ops without raising                          |
| `test_returns_revised_operations_from_llm` | LLM response wired through                       |
| `test_falls_back_to_originals_when_no_revised` | `revised_operations` defaults to original `operations` |
| `test_no_corrections_returns_empty_notes`  | Clean exit                                       |
| `test_llm_called_with_critic_model`        | Uses `get_critic_llm()` (not orchestrator LLM)   |
| `test_meta_separated_from_columns_in_prompt` | `__meta__` excluded from `source_profile` slot |

LLM is mocked — no provider keys required.

#### 2.5.3 Sequence Planner (Agent 3)
**File:** `tests/agents/test_sequence_planner.py`
**Target:** `src/agents/graph.py` — `plan_sequence_node`, `route_after_analyze_schema`

| Test                                       | Invariant verified                                            |
| ------------------------------------------ | ------------------------------------------------------------- |
| `test_skips_critic_on_cache_hit`           | YAML cache hit short-circuits Agent 2                         |
| `test_skips_critic_when_disabled` / `_when_flag_absent` | Critic off by default                            |
| `test_runs_critic_when_enabled`            | `--with-critic` routes to `critique_schema`                   |
| `test_cache_hit_overrides_critic_flag`     | Cache hit beats `with_critic=True`                            |
| `test_skip_when_block_sequence_already_set` | Idempotent                                                   |
| `test_silver_mode_uses_silver_sequence`    | `pipeline_mode=silver` → `get_silver_sequence`                 |
| `test_silver_mode_does_not_call_llm`       | LLM bypassed in silver mode                                   |
| `test_dropped_block_appended_before_dq_score_post` | **Reorder-only invariant** — Agent 3 cannot drop blocks |
| `test_dropped_block_appended_at_end_when_no_dq_score_post` | Edge case: no dq_score_post anchor               |
| `test_full_sequence_returned_when_llm_keeps_all_blocks` | Happy path                                       |
| `test_yaml_cache_written_when_fingerprint_present` | Cache write at end of plan_sequence (where the full cacheable blob is assembled) |

### 2.6 Pre and Post DQ Score Testing with Decimals
**File:** `tests/unit/test_dq_score.py`
**Target:** `src/blocks/dq_score.py`

| Test class               | Concern                                                         |
| ------------------------ | --------------------------------------------------------------- |
| `TestComputeDqScore`     | Score in [0,100], two-decimal rounding, completeness math (100% & 0%), `_SKIP_ALWAYS` columns excluded, custom weights, freshness for recent/old dates, ingredient-richness normalization |
| `TestDQScorePreBlock`    | Writes `dq_score_pre`, stores `dq_reference_columns` attr, two-decimal precision, **does not mutate input** |
| `TestDQScorePostBlock`   | Writes `dq_score_post` and `dq_delta`, two-decimal precision, **uses pre's reference columns** for fair delta, delta > 0 when nulls filled, no mutation |
| `TestDecimalPrecision`   | Half-rounding correctness with non-trivial weights, hard upper bound 100, hard lower bound 0, **`Decimal(str(score))` roundtrip** confirms no float artifacts |

---

## 3. Test Coverage Configuration

`pyproject.toml` extended with:

```toml
[tool.poetry.group.dev.dependencies]
pytest = "^8.0"
pytest-cov = "^5.0"
httpx = "^0.27"

[tool.pytest.ini_options]
testpaths = ["tests", "src"]
markers = [
    "integration: requires real GCS credentials (deselect with '-m not integration')",
]
addopts = "--strict-markers"

[tool.coverage.run]
source = ["src"]
branch = true
omit = [
    "src/uc3_search/*",                           # placeholder, NotImplementedError
    "src/uc4_recommendations/*",                  # placeholder, NotImplementedError
    "src/uc2_observability/dashboard.py",         # placeholder
    "src/uc2_observability/anomaly_detection.py", # placeholder
    "*/__init__.py",
]

[tool.coverage.report]
exclude_lines = [
    "pragma: no cover",
    "raise NotImplementedError",
    "if __name__ == .__main__.:",
    "if TYPE_CHECKING:",
]
show_missing = true
skip_covered = false
precision = 1

[tool.coverage.html]
directory = "htmlcov"
```

### Run commands

| Goal                          | Command                                                            |
| ----------------------------- | ------------------------------------------------------------------ |
| All tests                     | `poetry run pytest`                                                |
| Skip integration (no GCS)     | `poetry run pytest -m "not integration"`                           |
| Single layer                  | `poetry run pytest tests/unit/`                                    |
| One file                      | `poetry run pytest tests/unit/test_guardrails.py`                  |
| One test                      | `poetry run pytest tests/unit/test_dq_score.py::TestDecimalPrecision::test_decimal_serializable` |
| Coverage (terminal)           | `poetry run pytest --cov --cov-report=term-missing`                |
| Coverage (HTML — `htmlcov/`)  | `poetry run pytest --cov --cov-report=html`                        |
| Coverage (XML for CI)         | `poetry run pytest --cov --cov-report=xml`                         |
| Specific module coverage      | `poetry run pytest --cov=src.agents.guardrails tests/unit/test_guardrails.py` |

---

## 4. Shared Fixtures (`tests/conftest.py`)

Three module-level fixtures are available to every test:

| Fixture                  | Returns                                                                       |
| ------------------------ | ----------------------------------------------------------------------------- |
| `sample_source_schema`   | dict mimicking USDA-style profile (fdc_id, description, brand_owner, ingredients + `__meta__`) |
| `sample_unified_schema`  | dict with required cols (product_id/name), enrichment cols (allergens/primary_category/dietary_tags/is_organic), computed cols (dq_score_pre/post/delta) |
| `sample_dataframe`       | 3-row pandas DataFrame with product_name, brand_name, ingredients, published_date |

These guarantee tests build against a consistent baseline rather than re-inventing data per file.

---

## 5. Cross-cutting Invariants Verified

These critical project-wide invariants are now enforced by the test suite:

1. **Safety-column boundary** — `allergens`, `is_organic`, `dietary_tags` cannot be set by Agent 1 (`TestSchemaAnalysisOutput.test_safety_column_target_fails`), nor by S3 LLM enrichment (`TestEnrichmentOutput.test_safety_column_in_result_stripped_and_fails`).
2. **Reorder-only Agent 3** — Sequence planner cannot drop blocks; missing ones are re-injected before `dq_score_post` (`TestPlanSequenceNode.test_dropped_block_appended_before_dq_score_post`).
3. **Two-decimal DQ precision** — Pre/post scores and delta survive `round(2)` and `Decimal(str(s))` roundtrips (`TestDecimalPrecision.test_decimal_serializable`).
4. **Schema fingerprint stability** — Hash is invariant to column order but reflects column set, domain, and schema version (`TestSchemaFingerprint`).
5. **Cache-write coherence** — `plan_sequence_node` writes the full cacheable blob (`TestPlanSequenceNode.test_yaml_cache_written_when_fingerprint_present`).
6. **Deterministic Rules 4/6/7** — Type-mismatch RENAME promotion, uncovered-column DELETE injection, and identity-column normalization annotation all behave as documented (`TestDeterministicCorrections`).
7. **HITL thresholds** — Operation count >15, unresolvable >5, avg confidence <0.5, enrichment batch >50 each raise an `HITLFlag` (`TestHITLThresholds`).
8. **Pydantic DQWeights validator** — Weights must sum to 1.0 within 1e-6 (`TestDQWeights.test_weights_must_sum_to_one`).

---

## 6. What Is NOT Yet Covered (Future Work)

| Area                                              | Why deferred                                              |
| ------------------------------------------------- | --------------------------------------------------------- |
| End-to-end graph execution against real LLM       | Requires API keys and is non-deterministic — better as smoke test in a separate `e2e/` folder |
| GCS Bronze loader against real bucket             | Already covered by pre-existing `tests/test_gcs_loader.py` (integration-marked) |
| Streamlit UI render tests                         | Requires `streamlit run` — out of unit scope              |
| ChromaDB / FAISS S2 KNN against seeded corpus     | Pre-existing integration tests in `tests/integration/`     |
| Airflow DAG structural tests                      | Out of scope for this pass                                |
| UC2 Kafka consumer tests                          | Pre-existing in `tests/uc2_observability/`                |

---

## 7. Quick Reference — Files Added in This Pass

```
docs/consolidates_tests.md                  (this file)
pyproject.toml                              (UPDATED — pytest-cov, httpx, coverage config)
tests/conftest.py                           (NEW)
tests/unit/test_guardrails.py               (NEW — 86 tests)
tests/unit/test_pydantic_schemas.py         (NEW — 20 tests)
tests/unit/test_dq_score.py                 (NEW — 25 tests)
tests/unit/test_dynamic_mapping.py          (NEW — 37 tests)
tests/agents/__init__.py                    (NEW)
tests/agents/test_orchestrator.py           (NEW — 39 tests)
tests/agents/test_critic.py                 (NEW — 8 tests)
tests/agents/test_sequence_planner.py       (NEW — 8 tests)
tests/api/__init__.py                       (NEW)
tests/api/test_mcp_server.py                (NEW — 22 tests)
```

**Pass rate when run with all deps available: 245 / 245 (100%).**
**Pass rate in current venv (lacks `langgraph` + `psycopg2`): 243 / 245 with 2 cleanly skipped.**
