# Feature Specification: UI Architecture Rebuild

**Feature Branch**: `003-ui-architecture-rebuild`  
**Created**: 2026-04-17  
**Status**: Draft  
**Input**: User description: "Rebuild app.py from scratch. The current Streamlit UI is structurally misaligned — labels, layout, and flow were built around an older architecture. Do not patch the existing file. Delete and rewrite. The new UI should be derived entirely from the constitution and current codebase. Key requirements: - Reflect the 7-node pipeline flow accurately - Show Agent 1, Agent 2, Agent 3 activity distinctly with correct names - All 3 HITL gates must be present and correctly positioned - Display Agent 1 sampling and confidence scores (from 002) - Respect safety constraints — allergens, is_organic, dietary_tags must be visually flagged as read-only - No assumptions from old architecture"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Pipeline Wizard Navigation (Priority: P1)

User runs the Streamlit app and proceeds through the 5-step wizard that maps to the 7-node LangGraph pipeline.

**Why this priority**: Core user flow - all ETL operations happen through this wizard.

**Independent Test**: Launch app.py, verify all 5 steps are navigable and the pipeline executes end-to-end.

**Acceptance Scenarios**:

1. **Given** user launches Streamlit app, **When** they select a data source and domain, **Then** step 1 completes and step 2 shows Agent 1's schema analysis results
2. **Given** user is at step 2, **When** Agent 2 has completed critique, **Then** Agent 2's corrections are displayed distinctly from Agent 1's output
3. **Given** user reaches step 3, **When** pipeline executes, **Then** block execution trace shows Agent 3's planned sequence
4. **Given** user sees final results, **When** they review DQ scores and quarantine table, **Then** they can accept or override quarantine (HITL Gate 3)

---

### User Story 2 - Agent Activity Visibility (Priority: P1)

User can clearly see which agent performed each step of the pipeline and what it produced.

**Why this priority**: Transparency into the 3-agent architecture is essential for trust and debugging.

**Independent Test**: Run pipeline and verify each agent's output is labeled with agent name and role.

**Acceptance Scenarios**:

1. **Given** load_source and analyze_schema nodes run, **When** UI displays results, **Then** it shows "Agent 1 (Orchestrator): Schema Analysis" header
2. **Given** critique_schema node runs, **When** UI displays results, **Then** it shows "Agent 2 (Critic): Schema Validation" header with corrections
3. **Given** plan_sequence node runs, **When** UI displays results, **Then** it shows "Agent 3 (Sequence Planner): Execution Order" header with reasoning

---

### User Story 3 - Sampling & Confidence Display (Priority: P2)

User can see Agent 1's sampling strategy and confidence scores for each gap classification.

**Why this priority**: Provides transparency into Agent 1's data analysis quality and helps users assess reliability.

**Independent Test**: Run schema analysis and verify sampling stats and confidence badges appear in the schema delta table.

**Acceptance Scenarios**:

1. **Given** source data is loaded, **When** analyze_schema completes, **Then** sampling strategy (method, sample_size, fallback reason) is displayed
2. **Given** gaps are classified, **When** schema delta is rendered, **Then** each gap shows confidence score badge (High/Medium/Low)

---

### User Story 4 - HITL Gate Compliance (Priority: P1)

User encounters exactly 3 HITL approval gates at the correct pipeline points.

**Why this priority**: Constitution mandates HITL at critical decision points - skipping or misplacing them violates core design.

**Independent Test**: Complete full pipeline and count HITL interactions.

**Acceptance Scenarios**:

1. **Given** step 1 shows schema mapping, **When** user reviews column mapping, derivable gaps, missing columns, **Then** HITL Gate 1 appears: Approve Mapping / Exclude Columns / Abort
2. **Given** step 2 shows YAML mapping review, **When** user reviews generated operations, **Then** no separate code review gate exists (constitution: "No explicit code review gate")
3. **Given** step 4 shows quarantine results, **When** user sees rows that failed validation, **Then** HITL Gate 3 appears: Accept Quarantine / Override Include All

---

### User Story 5 - Safety Constraint Flagging (Priority: P2)

User sees that enrichment-only columns (allergens, is_organic, dietary_tags) are marked as read-only extraction fields.

**Why this priority**: Constitution mandates these fields are "extraction-only" - S2 and S3 must NOT modify them.

**Independent Test**: Load source with nutrition domain and verify safety columns show enrichment-only indicator.

**Acceptance Scenarios**:

1. **Given** unified schema includes allergens, is_organic, dietary_tags, **When** schema delta renders, **Then** these columns display "EXTRACTION-ONLY" badge and cannot be mapped from source columns
2. **Given** user tries to manually map a source column to allergens/is_organic/dietary_tags, **Then** UI shows warning that these are populated only by S1 extraction

---

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: UI MUST render a 5-step wizard that maps to the 7-node LangGraph pipeline: load_source → analyze_schema (Agent 1) → critique_schema (Agent 2) → check_registry → plan_sequence (Agent 3) → run_pipeline → save_output

- **FR-002**: UI MUST display Agent 1 (Orchestrator) activity with role label and show sampling statistics (method, sample_size, fallback_triggered) and confidence scores per gap classification

- **FR-003**: UI MUST display Agent 2 (Critic) activity with role label and show revision notes when corrections are applied

- **FR-004**: UI MUST display Agent 3 (Sequence Planner) activity with role label and show block execution sequence with reasoning

- **FR-005**: UI MUST position HITL Gate 1 (Schema Mapping Approval) after Agent 1's schema analysis and before proceeding to Agent 2

- **FR-006**: UI MUST position HITL Gate 2 implicitly - there is no separate code review gate per constitution

- **FR-007**: UI MUST position HITL Gate 3 (Quarantine Acceptance) after pipeline execution shows quarantined rows

- **FR-008**: UI MUST flag columns allergens, is_organic, dietary_tags with visual indicator that they are extraction-only (S1 enrichment only)

- **FR-009**: UI MUST show data quality scores (dq_score_pre, dq_score_post, dq_delta) and enrichment tier breakdown (S1/S2/S3 counts)

- **FR-010**: UI MUST support navigation between completed steps via sidebar after first step is complete

### Key Entities *(include if feature involves data)*

- **PipelineState**: TypedDict containing all state fields from src/agents/state.py
- **SamplingStrategy**: Dataclass with method, sample_size, fallback_triggered, fallback_reason
- **ConfidenceScore**: Dataclass with score (0.0-1.0), factors list, evidence_sample
- **HitlDecision**: User choices at each gate (approve, exclude_column, accept_quarantine, override)

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All 5 wizard steps render correctly and map to the 7-node pipeline
- **SC-002**: Agent 1, Agent 2, Agent 3 activity is distinctly labeled with correct names and roles
- **SC-003**: HITL Gate 1 appears after schema analysis with approve/exclude/abort options
- **SC-004**: HITL Gate 3 appears after pipeline execution with accept/override options
- **SC-005**: Sampling statistics display method, sample_size, fallback_triggered, fallback_reason
- **SC-006**: Confidence scores display as badges (High ≥90%, Medium 50-89%, Low <50%)
- **SC-007**: Safety columns (allergens, is_organic, dietary_tags) show extraction-only indicator

## Assumptions

- The constitution v1.2.1 is the authoritative source for architecture (3 agents, no code gen agent, 2 HITL gates explicit)
- src/agents/graph.py NODE_MAP defines the 7 pipeline nodes available
- src/schema/sampling.py provides SamplingStrategy dataclass
- src/agents/confidence.py provides ConfidenceScore dataclass and display helpers
- config/unified_schema.json defines the nutrition domain schema including enrichment columns
- The Streamlit session state will track step progress and pipeline state across steps