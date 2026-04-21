# Tasks: GCS Bronze Layer Connector

**Input**: `specs/007-gcs-connector/` (plan.md, spec.md, research.md, data-model.md, contracts/)

**Context**: Core loader, CLI, checkpoint, and orchestrator integration are already implemented. Tasks below close the three gaps identified in plan.md and update tests to match the clarified spec.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Parallelizable (different files, no blockers from incomplete tasks)
- **[Story]**: US1 = core loader gaps; US2 = unit test updates; US3 = integration & docs

---

## Phase 1: Setup

**Purpose**: No new project structure needed — `src/pipeline/loaders/`, `tests/`, and `specs/007-gcs-connector/` already exist.

- [x] T001 Verify `google-cloud-storage ^3.10.1` and `tenacity` (or equivalent) in `pyproject.toml` — add if missing

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Understand current `GCSSourceLoader` behavior before patching. No code changes yet.

**⚠️ CRITICAL**: Read before writing — existing tests must be understood so updates don't break passing cases.

- [x] T002 Read `src/pipeline/loaders/gcs_loader.py` in full — map current call paths for `_list_blobs`, `_blob_to_df`, `load_sample`, `iter_chunks`
- [x] T003 Run existing unit tests to confirm green baseline: `cd src && pytest ../tests/test_gcs_loader.py -v`

**Checkpoint**: Baseline green — safe to patch loader

---

## Phase 3: User Story 1 — Core Loader Hardening (Priority: P1) 🎯 MVP

**Goal**: `GCSSourceLoader` raises immediately on zero-file match, streams large blobs line-by-line without full materialization, retries transient GCS errors 3× with exponential backoff, and serializes nested dict/list values to JSON strings.

**Independent Test**: `pytest ../tests/test_gcs_loader.py -v` — all unit tests pass, including the updated zero-file and new retry/streaming tests.

### Implementation for User Story 1

- [x] T004 [US1] In `src/pipeline/loaders/gcs_loader.py` `_list_blobs()`: replace `logger.warning` + `return []` with `raise FileNotFoundError(f"No blobs matched GCS pattern: {self.uri_pattern}")`
- [x] T005 [US1] In `src/pipeline/loaders/gcs_loader.py` `_blob_to_df()`: replace `blob.download_as_bytes()` + `pd.read_json(io.BytesIO(...))` with `blob.open("rb")` line-by-line iteration — accumulate records list, return `pd.DataFrame(records)`
- [x] T006 [US1] In `src/pipeline/loaders/gcs_loader.py` `_blob_to_df()`: after building DataFrame, detect columns where first non-null value is `dict` or `list`, apply `json.dumps` to serialize nested values to strings
- [x] T007 [US1] In `src/pipeline/loaders/gcs_loader.py`: add `_with_retry(fn, *args, **kwargs)` helper — 3 attempts, exponential backoff (1s → 2s → 4s via `time.sleep`), re-raise on final failure; wrap `_list_blobs()` GCS calls and `blob.open()` / `download_as_bytes()` in `_blob_to_df()`

**Checkpoint**: Core loader hardened — `load_sample()` and `iter_chunks()` satisfy all clarified spec requirements

---

## Phase 4: User Story 2 — Unit Test Updates (Priority: P2)

**Goal**: Unit tests reflect clarified spec: zero-file match raises `FileNotFoundError`, large partitions stream correctly, retry behavior is verified, nested JSON serializes to strings.

**Independent Test**: `pytest ../tests/test_gcs_loader.py -v` — all tests pass including new assertions.

### Implementation for User Story 2

- [x] T008 [P] [US2] In `tests/test_gcs_loader.py`: update `test_load_sample_empty_bucket_returns_empty_df` — change assertion from `assert df.empty` to `pytest.raises(FileNotFoundError)`
- [x] T009 [P] [US2] In `tests/test_gcs_loader.py`: add `test_iter_chunks_no_match_raises_error` — mock `list_blobs` returns `[]`, assert `FileNotFoundError` raised when calling `list(loader.iter_chunks())`
- [x] T010 [P] [US2] In `tests/test_gcs_loader.py`: add `test_blob_to_df_serializes_nested_json` — mock blob returns JSONL with a nested dict field; assert resulting DataFrame column contains JSON string, not dict
- [x] T011 [US2] In `tests/test_gcs_loader.py`: add `test_retry_succeeds_after_transient_failure` — mock `blob.open` (or `download_as_bytes`) to raise `Exception` twice then succeed; assert DataFrame returned and mock called 3×
- [x] T012 [US2] In `tests/test_gcs_loader.py`: add `test_retry_raises_after_max_attempts` — mock `blob.open` to always raise; assert exception propagates after 3 calls

**Checkpoint**: All unit tests pass — loader behavior fully covered

---

## Phase 5: Polish & Cross-Cutting Concerns

- [x] T013 [P] Add integration test to `tests/test_gcs_loader.py`: mark with `@pytest.mark.integration`; reads `gs://mip-bronze-2024/usda/2026/04/20/part_0000.jsonl` via real GCS, verifies DataFrame non-empty and columns match Orchestrator expectations — skip if `GOOGLE_CLOUD_PROJECT` not set
- [x] T014 [P] Update `specs/007-gcs-connector/quickstart.md` integration test section with exact pytest command: `pytest tests/test_gcs_loader.py -v -m integration`
- [x] T015 Run full pipeline CLI smoke test per quickstart.md: `python -m src.pipeline.cli --source gs://mip-bronze-2024/usda/2026/04/20/part_0000.jsonl --domain nutrition`
- [x] T016 Validate constitution alignment: confirm `src/agents/orchestrator.py` `load_source` node still calls `loader.load_sample()` and handles `FileNotFoundError` correctly (propagates to user with clear message)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — start immediately
- **Phase 2 (Foundational)**: Depends on Phase 1 — BLOCKS US1 and US2
- **Phase 3 (US1)**: Depends on Phase 2 — implement T004 → T005 → T006 → T007 sequentially (all in same file)
- **Phase 4 (US2)**: Depends on Phase 3 complete (tests must match implemented behavior)
- **Phase 5 (Polish)**: Depends on Phases 3 + 4

### Within User Story 1

- T004 before T005 (both touch `_list_blobs` / `_blob_to_df` — do in order to avoid conflict)
- T005 before T006 (nested JSON detection runs after DataFrame is built)
- T007 wraps functions already modified by T004–T006 — implement last

### Parallel Opportunities

- T008, T009, T010 (test file, different test functions) — can run in parallel
- T013, T014 (different files) — can run in parallel

---

## Parallel Example: User Story 2

```bash
# All three independent test additions can be drafted simultaneously:
Task: "test_load_sample_empty_bucket raises FileNotFoundError  (T008)"
Task: "test_iter_chunks_no_match raises FileNotFoundError      (T009)"
Task: "test_blob_to_df_serializes_nested_json                  (T010)"
```

---

## Implementation Strategy

### MVP (US1 only — T001–T007)

1. Phase 1: verify deps
2. Phase 2: read + baseline green
3. Phase 3: patch loader (T004 → T005 → T006 → T007)
4. **STOP**: run `pytest ../tests/test_gcs_loader.py -v` — confirm existing tests still pass (some will now fail on the zero-file case — that's expected, fix in US2)

### Incremental Delivery

1. US1 (T001–T007): loader hardened
2. US2 (T008–T012): test suite updated → full green
3. Polish (T013–T016): integration test + docs

---

## Notes

- T005 streaming change: `blob.open("rb")` returns a streaming `BlobReader`; iterate lines with `for line in blob_stream`, decode UTF-8, `json.loads` each line
- T007 retry helper: use `time.sleep` — no new dependency needed; retry only wraps the GCS I/O calls, not the DataFrame operations
- T004 change breaks `test_load_sample_empty_bucket_returns_empty_df` — expected, fixed in T008
- Integration test (T013) requires `gcloud auth application-default login` — skip gracefully if creds absent
