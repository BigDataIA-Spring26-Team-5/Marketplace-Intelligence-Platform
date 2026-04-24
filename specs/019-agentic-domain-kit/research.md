# Research: Agentic Domain Kit Builder

## 1. LangGraph graph-per-agent pattern

**Decision**: Two separate `StateGraph` instances — `DomainKitGraph` (8 nodes) and `ScaffoldGraph` (5 nodes) — each with its own `TypedDict` state.

**Rationale**: Re-using `PipelineState` would bloat it with irrelevant keys. Separate graphs keep state minimal, allow `run_step(node, state)` to be called identically to the existing pattern in `app.py`, and avoid touching `src/agents/graph.py`.

**Alternatives considered**: Single combined graph rejected because the scaffold flow has a completely different lifecycle; one-shot function calls rejected because spec requires HITL between nodes (FR-3, FR-7).

---

## 2. Where to place the new graph module

**Decision**: `src/agents/domain_kit_graph.py` — new file, not a modification of `src/agents/graph.py`.

**Rationale**: Spec FR-4 explicitly prohibits modifying `src/agents/prompts.py`; by analogy, the existing `graph.py` must not change. Placing the new file in `src/agents/` keeps LangGraph components co-located.

**Alternatives considered**: `src/ui/` rejected — graph logic does not belong in UI layer. New top-level `src/domain_kit/` package rejected as over-engineering for two files.

---

## 3. Prompt placement

**Decision**: `src/agents/domain_kit_prompts.py` — new module, standalone.

**Rationale**: FR-4 mandates a dedicated prompt module separate from `src/agents/prompts.py`. Domain-agnostic language enforced by code review (no literal domain field names as string literals; use template variables).

**Alternatives considered**: Inline strings in `domain_kit_graph.py` rejected — would make prompts hard to review and adjust independently.

---

## 4. Validation rules (FR-6) reuse between enrichment validator and Preview tab

**Decision**: Extract deterministic checks into a standalone function `validate_enrichment_rules(yaml_dict, csv_headers) -> list[ValidationIssue]` in `src/agents/domain_kit_graph.py`. Preview/Validate tab imports and calls the same function.

**Rationale**: Spec § Assumptions: "the `validate_enrichment_rules` node reuses the same deterministic rules as FR-6." One implementation, two callers.

**Alternatives considered**: Duplicate implementations rejected — divergence would be a maintenance hazard.

---

## 5. Retry loop structure within LangGraph

**Decision**: Conditional edges on `retry_count` field in state: after `validate_enrichment_rules`, route to `revise_enrichment_rules` if errors exist and `retry_count < 2`, else to `generate_prompt_examples`.

**Rationale**: LangGraph's conditional routing handles retry without while-loops or recursion. State carries `retry_count` and `validation_errors` so the fix prompt can include the previous errors.

**Alternatives considered**: Python while-loop inside a single node rejected — Streamlit step-by-step integration requires each node to be a separate graph node.

---

## 6. HITL gate implementation

**Decision**: `hitl_review` node in `DomainKitGraph` does not call any LLM — it is a no-op node that transitions state to `pending_review=True`. Streamlit renders editable text areas and gated "Approve" button; on approval it merges user edits into state and calls `run_step("commit_to_disk", state)`.

**Rationale**: Matches existing pattern in `app.py` where `_run_step("check_registry")` is called only after the user approves the HITL gate. No new HITL infrastructure needed (spec § Assumptions).

**Alternatives considered**: Separate Streamlit page rejected — navigation complexity, no benefit.

---

## 7. Streamlit session state key scoping

**Decision**: Use `st.session_state["domain_kit_state"]` and `st.session_state["scaffold_state"]` as separate dicts, never merged with `st.session_state.pipeline_state`.

**Rationale**: Avoids key collisions with the main pipeline state. Tab switching resets only the relevant sub-dict.

---

## 8. "Run Pipeline" post-commit navigation bug (FR-8)

**Decision**: Fix by writing the target mode to `st.session_state["_mode_override"]` then calling `st.rerun()`. In `app.py` sidebar radio rendering, check for `_mode_override` first and consume it (pop after reading).

**Rationale**: Streamlit radio widget doesn't honor `index=` changes to session state mid-run. A separate sentinel key that is consumed on next render is the canonical workaround.

**Alternatives considered**: `st.query_params` rejected — requires URL manipulation which breaks in embedded contexts.

---

## 9. Preview validator expanded checks (FR-6)

Deterministic checks in order:
1. `__generated__` sentinel absent → ERROR
2. `dq_score_pre` not at position 0 or `dq_score_post` not at last position → WARNING
3. Block in sequence references `<domain>__<name>` but no `custom_blocks/<name>.py` exists → ERROR
4. Field in `enrichment_rules.yaml` matches a CSV header exactly → WARNING ("may re-extract structured column — use RENAME")
5. Same logical name in both `enrichment_rules` fields and a custom block reference in sequence → WARNING ("double-extraction anti-pattern")

Checks 4 and 5 require a CSV header list; Preview UI must accept a CSV upload or use the fixture headers if a fixture was used during generation.

---

## 10. Existing code to replace vs. extend

| Current file | Disposition |
|---|---|
| `src/ui/kit_generator.py` | **Replace** — single-shot LLM, no retry, no graph |
| `src/ui/block_scaffolder.py` | **Replace** — single-shot, no retry loop |
| `src/ui/domain_kits.py` | **Extend** — keep tab structure; rewire Generate and Scaffold tabs to call new graphs |

Neither `src/agents/graph.py` nor `src/agents/prompts.py` is touched.
