# Contract: src/agents/domain_kit_graph.py

## Public surface

```python
# State types
class DomainKitState(TypedDict, total=False): ...
class ScaffoldState(TypedDict, total=False): ...
class ValidationIssue(TypedDict):
    level: str   # "error" | "warning"
    check: str
    message: str

# Deterministic validator — used by both validate_enrichment_rules node and Preview tab
def validate_enrichment_rules_yaml(
    yaml_dict: dict,
    csv_headers: list[str],
) -> list[ValidationIssue]: ...

# Step runner — same signature as graph.py's run_step
def run_kit_step(step_name: str, state: DomainKitState) -> DomainKitState: ...
def run_scaffold_step(step_name: str, state: ScaffoldState) -> ScaffoldState: ...

# Graph builders (for testing)
def build_kit_graph() -> StateGraph: ...
def build_scaffold_graph() -> StateGraph: ...
```

## Invariants

- `run_kit_step` and `run_scaffold_step` MUST NOT write any files to disk.
- `commit_to_disk` node writes ONLY to `domain_packs/<domain>/` and ONLY when called explicitly.
- `validate_enrichment_rules_yaml` is pure (no LLM, no I/O, no side effects).
- Retry counter increments in `validate_enrichment_rules` / `validate_syntax` nodes, not in the fix nodes.
- All LLM calls use `get_orchestrator_llm()` from `src.models.llm`.
- No import from `src.agents.graph` or `src.agents.prompts`.
