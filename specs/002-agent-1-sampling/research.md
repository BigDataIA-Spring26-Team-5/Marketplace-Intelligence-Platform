# Research: Agent 1 Representative Sampling

## Decision 1: Sampling Method for Schema Detection

**Chosen**: Stratified random sampling with adaptive minimum

**Rationale**: For schema detection, we need to ensure we capture:
- All unique values for categorical columns (≥95% coverage goal from SC-002)
- Non-null values for sparse columns
- JSON/structured data patterns

**Approach**:
1. **Minimum sample**: 500 rows (ensures reasonable probability of capturing sparse values)
2. **For datasets < 500 rows**: Full scan (100% sampling)
3. **For datasets > 500 rows**: Random sampling with minimum 500 or sqrt(n), whichever is larger
4. **Stratified fallback**: If column shows >80% null rate in initial sample, switch to full scan

**Alternatives evaluated**:
- Simple random: May miss sparse values
- Systematic (every Nth): Same issue as random for sparse data
- Reservoir sampling: Good for streaming, but overkill for CSV batch
- **Chosen**: Stratified with fallback — best balance of accuracy vs cost

---

## Decision 2: Confidence Scoring for LLM Gap Classification

**Chosen**: Multi-factor heuristic scoring based on evidence strength

**Rationale**: LLM doesn't natively provide confidence scores, so we derive them from:
1. **Null rate in sample**: Low null → high confidence
2. **Value consistency**: Uniform values → high confidence
3. **Type consistency**: All values same type → high confidence
4. **Sample coverage**: Sample contains evidence → higher confidence

**Formula**:
```
confidence = (1 - null_rate) * type_consistency * evidence_present
```

Where:
- `null_rate`: % of nulls in sampled column
- `type_consistency`: 1.0 if all values same dtype, 0.5 if mixed
- `evidence_present`: 1.0 if sample shows any non-null values

**Output**: confidence_score (0.0 - 1.0) attached to each GapItem

**Alternatives evaluated**:
- Ask LLM to self-rate: Unreliable, LLM overconfident
- Token probability: Not available in standard LLM API
- **Chosen**: Heuristic based on data characteristics — deterministic and explainable

---

## Decision 3: Adaptive Sample Size Formula

**Chosen**: min(500, sqrt(n)) + buffer for sparse columns

**Rationale**: Balance between:
- Too small → miss sparse values (current problem)
- Too large → waste tokens (成本)
- Formula must scale gracefully from 100 to 1M rows

**Formula**:
```python
def calculate_sample_size(total_rows: int) -> int:
    base = min(500, int(math.sqrt(total_rows)))
    
    # Buffer for sparse column detection
    # If dataset has many potential sparse columns, increase sample
    buffer = min(200, total_rows // 20)  # Up to 200 extra rows or 5%
    
    return min(base + buffer, total_rows)  # Never exceed total
```

**Examples**:
| Total Rows | Sample Size | Rationale |
|------------|-------------|-----------|
| 100 | 100 | Full scan acceptable |
| 500 | 500 | Full scan |
| 1,000 | 550 | sqrt(1000) ≈ 316, +buffer = 550 |
| 10,000 | 700 | sqrt(10K)=100, +buffer=300 |
| 100,000 | 700 | capped at 700 |
| 1,000,000 | 700 | capped at 700 |

**Token impact**: With ~50 columns profiled, 700 rows × 50 = 35K cells → ~10K tokens, within 2× baseline

**Alternatives evaluated**:
- Fixed 1000: Too large for small datasets
- Linear (10%): Too large for 1M rows
- **Chosen**: sqrt + buffer — scales appropriately, proven in data science

---

## Summary

| Decision | Chosen | Rationale |
|----------|--------|-----------|
| Sampling method | Stratified random with full-scan fallback | Captures sparse values while managing cost |
| Confidence scoring | Multi-factor heuristic (null_rate × type_consistency × evidence) | Deterministic, explainable, no LLM self-rating |
| Sample size formula | min(500, sqrt(n)) + buffer | Scales 100→1M rows within 2× token budget |

All decisions align with Constitution Principle I (Schema-First Gap Analysis) — sampling improves gap detection accuracy without changing the 8-primitive taxonomy or YAML-driven approach.