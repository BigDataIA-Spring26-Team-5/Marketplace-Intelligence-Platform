<!--
Sync Impact Report:
- Version: 2.0.0 -> 2.1.0
- Modified principles: None (existing principles unchanged)
- Added sections:
    - Principle X: Domain Packs as the Extension Layer
- Removed sections: None
- Templates requiring updates:
    - ✅ `.specify/templates/plan-template.md` — stale `config/unified_schema.json` reference in
      Constitution Check gate corrected to `config/schemas/<domain>_schema.json`; Domain Pack
      extension compliance gate added
    - ✅ `.specify/templates/spec-template.md` — no domain-pack references present; no change needed
    - ✅ `.specify/templates/tasks-template.md` — no stale references; no change needed
- Rationale for MINOR bump: Principle X adds a mandatory governance rule about the Domain Pack
  extension layer. No existing principles were redefined or removed.
- Follow-up TODOs: None — all Domain Pack artifacts are implemented and verified.
-->
# Schema-Driven ETL Pipeline Constitution

## Core Principles

### I. Schema-First Gap Analysis
Every ingestion flow MUST analyze the incoming dataset against
`config/schemas/<domain>_schema.json` before transformation planning begins.
The domain is always operator-supplied via `PipelineState["domain"]` and MUST
NOT be inferred or auto-classified by any agent. Schema gaps MUST be classified
with the 8-primitive taxonomy: `RENAME`, `CAST`, `FORMAT`, `DELETE`, `ADD`,
`SPLIT`, `UNIFY`, and `DERIVE`. Agent 1 MUST produce the initial operations
list, and Agent 2 MUST review that list before YAML mapping registration
proceeds.

Rationale: the domain schema file is the contract for all downstream blocks,
data quality scoring, and output validation. A single global schema cannot
represent the distinct column sets of nutrition, safety, and pricing domains
without conflating semantically different fields.

### II. Three-Agent Pipeline with Critic Review
The pipeline architecture MUST remain a three-agent flow with distinct
responsibilities:
- **Agent 1 (Orchestrator)**: analyze source schema and propose gap operations
- **Agent 2 (Critic)**: audit and correct Agent 1 output with a reasoning model
- **Agent 3 (Sequence Planner)**: choose block order from the available pool

No agent may generate executable transformation code at runtime. Agent 3 MAY
reorder blocks, but it MUST NOT add or remove blocks from the available pool.

Rationale: explicit role boundaries keep LLM behavior auditable and prevent
architecture drift back to code generation.

### III. Declarative YAML Execution Only
Schema transformations MUST execute through declarative YAML mappings consumed by
`DynamicMappingBlock`. All supported primitives MUST compile to a known YAML
operation or to an explicit null/default fallback before the pipeline runs.
Runtime-generated Python transformation blocks are prohibited.

Generated mapping files MUST be written under
`src/blocks/generated/<domain>/DYNAMIC_MAPPING_<dataset>.yaml` and MUST be
treated as the source of truth for dataset-specific transformations.

Rationale: YAML-only execution provides deterministic behavior, reviewable
artifacts, and replay without sandbox risk.

### IV. Human Approval Gates
Human review MUST exist at the decision points that can materially change output
correctness:
- **Gate 1**: schema mapping review, including missing-column handling and
  schema exclusions
- **Gate 2**: quarantine review for rows that still fail required-field checks

There is no code-review gate for generated transforms because runtime code
generation is not allowed. Human decisions MUST be merged into the mapping state
before execution.

Rationale: these are the two points where operator intent changes the meaning of
the final dataset.

### V. Cascading Enrichment with Safety Boundaries
Enrichment MUST proceed in cost order:
1. `S1` deterministic extraction
2. `S2` KNN corpus search
3. `S3` RAG-assisted LLM categorization

`primary_category` MAY be resolved by `S1`, `S2`, or `S3`. Safety fields
(`allergens`, `dietary_tags`, `is_organic`, and any field a domain declares as
safety-only) MUST remain deterministic-only. They MUST NOT be inferred or
modified by `S2` or `S3`.

The set of safety fields is domain-defined via `EnrichmentRulesLoader`. The
kernel MUST query `safety_field_names()` at runtime — it MUST NOT hardcode
field names from any specific domain.

Rationale: category tolerates probabilistic inference; safety fields do not.
Hardcoding food-domain field names in kernel code violates domain isolation.

### VI. Self-Extending Mapping Memory
When a dataset-specific mapping is generated, it MUST be persisted and
auto-discoverable on future runs. Re-ingesting a known source SHOULD reuse the
existing mapping artifact and avoid repeating schema-analysis work unless the
schema contract has changed.

Rationale: replayability and cost control are core behavior, not an optimization
detail.

### VII. Data Quality and Quarantine Enforcement
The pipeline MUST compute `dq_score_pre` before enrichment and `dq_score_post`
after pipeline execution. Rows that still fail required-field validation after
enrichment and alias application MUST be quarantined, and quarantine reasons
MUST be recorded in machine-readable form.

Output files written to `output/` MUST contain only rows that passed required
field validation unless a human explicitly overrides quarantine handling.

Rationale: output acceptance must be measurable and traceable.

### VIII. Production Scale
The pipeline MUST meet production-scale operational requirements:

- **Volume**: 50k+ records MUST complete in a single run without hitting resource
  limits
- **Batch-only LLM**: Per-record LLM calls are prohibited - all LLM operations MUST
  operate on batched record windows, never on individual rows
- **Checkpointing**: Pipeline state MUST be checkpointed to durable storage
  after processing each chunk to enable resumption on failure
- **Batched enrichment**: Enrichment tiers S2 (KNN corpus search) and S3
  (LLM categorization) MUST use batched operations - individual record
  lookups are prohibited
- **Auto-approval**: Human approval gates MUST support confidence
  thresholds where records meeting or exceeding the configured confidence
  score auto-approve without human intervention
- **Configurable limits**: Maximum LLM calls per run MUST be configurable
  via YAML configuration (e.g., `config/limits.yaml`), not hardcoded

Rationale: production deployments require predictable cost, resumption
capability, and throughput that are incompatible with row-by-row LLM
calling.

### IX. Domain-Scoped Schemas, Silver Normalization, and Gold Concatenation
Domain schemas and pipeline-layer outputs MUST be organized and governed as
follows:

- Every domain MUST have exactly one schema file at
  `config/schemas/<domain>_schema.json`. This file defines the canonical column
  set for that domain and is the sole schema contract for all datasets belonging
  to that domain.
- Columns defined in one domain schema MUST NOT appear in another domain schema
  unless they are semantically identical and intentionally shared (e.g. `id`,
  `data_source`, `created_at`).
- During Bronze transformation, Agent 1 MUST perform gap analysis against the
  domain schema only. Columns not present in the domain schema MUST be
  classified for deletion using the `DELETE` primitive.
- When a Bronze dataset is missing a field required by the domain schema, the
  pipeline MUST fill it with a null or configured default value and MUST flag
  the affected rows in `dq_score_pre` before enrichment begins.
- All datasets within a domain MUST conform to an identical column set and
  column order after Silver normalization. Silver normalization is a fixed
  post-block-sequence step inside `run_pipeline`, not a registered block in
  `BlockRegistry`.
- When all Silver datasets for a domain have been processed, `run_pipeline`
  MUST concatenate them into a single Gold output file at
  `output/gold/<domain>.parquet` (or configured equivalent). Concatenation is
  domain-scoped — datasets from different domains are never concatenated
  together.
- The seven-node graph order (`load_source → analyze_schema → critique_schema →
  check_registry → plan_sequence → run_pipeline → save_output`) MUST NOT change.
  Silver normalization and Gold concatenation are internal responsibilities of
  `run_pipeline`.

Rationale: domain-scoped schemas prevent column-set bleed across semantically
distinct ingestion flows. Enforcing identical column sets at the Silver layer
makes Gold concatenation a safe append with no schema resolution required at
merge time.

### X. Domain Packs as the Extension Layer
All domain-specific behavior MUST be encapsulated in a Domain Pack located at
`domain_packs/<domain>/`. Adding a new domain MUST require zero edits to any
file under `src/`. The kernel MUST remain domain-agnostic.

A Domain Pack MUST provide:
- `enrichment_rules.yaml` — `text_columns`, `fields` list with `strategy`,
  `output_type`, and `patterns`; LLM fields additionally provide
  `classification_classes` and `rag_context_field`
- `block_sequence.yaml` — ordered block names including the `__generated__`
  sentinel; may also define `silver_sequence` and `gold_sequence` keys
- `prompt_examples.yaml` — few-shot column mapping examples for Agent 1

A Domain Pack MAY provide:
- `custom_blocks/*.py` — `Block` subclasses auto-discovered by `BlockRegistry`
  at init time via `importlib`; MUST follow the naming convention
  `<domain>__<block_name>` for the `name` attribute

The following kernel components MUST be fully parameterized by domain at
runtime — they MUST NOT contain hardcoded field names from any specific domain:
- `EnrichmentRulesLoader` — sole source of `text_columns`, enrichment column
  names, safety field names, LLM categories, and RAG context field
- `LLMEnrichBlock` — derives `enrich_cols` and `safety_fields` from
  `EnrichmentRulesLoader` at runtime
- `deterministic_enrich()` — derives `text_cols` from `EnrichmentRulesLoader`
  at runtime
- `llm_enrich()` — builds prompts from `EnrichmentRulesLoader` at runtime
- `validate_enrichment_output()` in `guardrails.py` — derives safety columns
  and valid categories from `EnrichmentRulesLoader` at runtime

**SC-002 invariant**: running the pipeline with `--domain <non-food-domain>`
MUST produce an output that contains zero columns from any other domain's
enrichment schema. This MUST be enforced by test.

Rationale: domain isolation is what makes the pipeline reusable across
healthcare, pharma, e-commerce, and other domains without forking `src/`. Any
leakage of domain-specific constants into kernel code re-introduces coupling
that SC-002 is designed to catch.

## Technology Stack

- **Language**: Python 3.11
- **Data Processing**: pandas
- **LLM Access**: LiteLLM
- **Primary Models**: DeepSeek chat model for Agent 1 and Agent 3; reasoning
  model for Agent 2 when available
- **Workflow Engine**: LangGraph
- **UI**: Streamlit
- **Similarity Search**: FAISS

The constitution governs behavior, not vendor lock-in. Equivalent replacements
are allowed only if they preserve the agent responsibilities and constraints in
the Core Principles.

## Development Workflow

The default non-interactive graph MUST preserve this seven-node order:
1. `load_source`
2. `analyze_schema`
3. `critique_schema`
4. `check_registry`
5. `plan_sequence`
6. `run_pipeline`
7. `save_output`

The interactive Streamlit flow MUST expose the approval gates before execution
commits operator decisions to the YAML mapping or accepts quarantined results.

Quality gates for any feature or refactor:
- domain-schema alignment is documented (`config/schemas/<domain>_schema.json`)
- YAML mapping behavior is explicit and testable
- enrichment safety fields remain deterministic-only
- replayed mappings under `src/blocks/generated/` still load correctly
- quarantine behavior and DQ scoring remain intact
- new domain support requires only `domain_packs/<domain>/` additions — zero
  `src/` edits
- SC-002: a test asserts zero foreign-domain enrichment columns in output when
  using a non-food domain
- README, templates, and agent guidance stay consistent with the architecture

## Governance

This constitution supersedes conflicting local conventions and feature plans.
Every implementation plan, specification, task list, and runtime guidance
document MUST pass a constitution review before work is considered ready.

Amendments require:
- a written description of the rule change
- rationale for the change
- propagation to affected templates and guidance documents
- a semantic version update for this constitution

Versioning policy:
- **MAJOR**: removes a principle, redefines architecture boundaries, or changes a
  non-negotiable rule in a backward-incompatible way
- **MINOR**: adds a principle, adds a mandatory governance section, or materially
  expands implementation obligations
- **PATCH**: clarifies wording without changing required behavior

Compliance review expectations:
- plans MUST state how the work satisfies the constitution gates
- specs MUST capture schema, HITL, enrichment, and quarantine implications when
  relevant
- tasks MUST include the work needed to preserve YAML mappings, DQ logic, and
  documentation consistency
- tasks for any new domain MUST include Domain Pack artifacts and SC-002
  compliance verification
- runtime guidance MUST not describe deprecated architecture such as runtime
  code generation or hardcoded domain field names in kernel code

**Version**: 2.1.0 | **Ratified**: 2026-04-17 | **Last Amended**: 2026-04-24

<!--
Changelog:
- 2.1.0 (2026-04-24): MINOR bump. Principle X added — Domain Packs as the Extension Layer.
  Establishes the zero-src-edit contract for new domains, mandates kernel parameterization
  via EnrichmentRulesLoader, defines SC-002 as a testable invariant, and lists the four
  required and one optional Domain Pack artifact types. Principle V amended: safety field
  set is now domain-defined via EnrichmentRulesLoader; hardcoding food-domain field names
  in kernel code explicitly prohibited. Development Workflow quality gates updated to
  include Domain Pack gate and SC-002 test gate.
- 2.0.0 (2026-04-22): MAJOR bump. Principle I amended — schema contract changed from
  config/unified_schema.json (single global file) to config/schemas/<domain>_schema.json
  (per-domain files); domain is always operator-supplied, never agent-inferred.
  config/unified_schema.json retired as a governance artifact. Principle IX added —
  domain-scoped schemas, Silver normalization as internal run_pipeline step, Gold
  concatenation scoped to domain. Development Workflow quality gate updated from
  "unified-schema alignment" to "domain-schema alignment". MAJOR because Principle I
  redefines a non-negotiable rule in a backward-incompatible way: all runs reading
  config/unified_schema.json break without migration.
- 1.4.0 (2026-04-18): Added Principle VIII (Production Scale).
- 1.3.0 (prior): Added Principles I–VII.
-->
