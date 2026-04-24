"""Domain Kits UI panel — Generate, Scaffold, Preview, and Manage domain packs."""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DOMAIN_PACKS_DIR = PROJECT_ROOT / "domain_packs"
FIXTURES_DIR = PROJECT_ROOT / "tests" / "fixtures"

_SLUG_RE = re.compile(r"^[a-z][a-z0-9_]*$")

# Fixture presets: (label, csv filename, suggested domain name, suggested description)
_FIXTURE_PRESETS: list[tuple[str, str, str, str]] = [
    (
        "healthcare_test — patient discharge records",
        "healthcare_sample.csv",
        "healthcare_test",
        "Patient discharge records with ICD-10 diagnosis codes, medication lists, and clinical procedures.",
    ),
    (
        "nutrition — food product catalog",
        "nutrition_sample.csv",
        "nutrition",
        "Branded food products with ingredient lists, allergens, dietary tags, and food category classification.",
    ),
    (
        "pharma — pharmaceutical drug registry",
        "pharma_sample.csv",
        "pharma",
        "Pharmaceutical products with NDC codes, active ingredients, dosage forms, and FDA approval status.",
    ),
    (
        "fda_recalls — food safety recall notices",
        "fda_recalls_sample.csv",
        "fda_recalls",
        "FDA food recall notices with recall classification, reason for recall, and distribution pattern.",
    ),
]

# ---------------------------------------------------------------------------
# Audit helpers (T014 / T026)
# ---------------------------------------------------------------------------

def _append_audit(domain_name: str, action: str, outcome: str, detail: str) -> None:
    """Append one JSON line to domain_packs/<domain>/.audit.jsonl."""
    audit_file = DOMAIN_PACKS_DIR / domain_name / ".audit.jsonl"
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "domain": domain_name,
        "action": action,
        "outcome": outcome,
        "detail": detail,
    }
    try:
        audit_file.parent.mkdir(parents=True, exist_ok=True)
        with open(audit_file, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as exc:
        logger.warning("Could not write audit entry for %s: %s", domain_name, exc)


def _load_audit_log(domain_name: str) -> list[dict]:
    """Return last 20 audit entries from domain_packs/<domain>/.audit.jsonl."""
    audit_file = DOMAIN_PACKS_DIR / domain_name / ".audit.jsonl"
    if not audit_file.exists():
        return []
    entries = []
    try:
        with open(audit_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        entries.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    except Exception as exc:
        logger.warning("Could not read audit log for %s: %s", domain_name, exc)
    return entries[-20:]


# ---------------------------------------------------------------------------
# Domain pack listing (T025)
# ---------------------------------------------------------------------------

def _is_builtin(domain_name: str) -> bool:
    """True if any file under domain_packs/<domain>/ is tracked by git."""
    try:
        result = subprocess.run(
            ["git", "ls-files", f"domain_packs/{domain_name}/"],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
            timeout=5,
        )
        return bool(result.stdout.strip())
    except Exception:
        return False


def _list_domain_packs() -> list[dict]:
    """Scan domain_packs/ and return metadata list for all domains."""
    if not DOMAIN_PACKS_DIR.exists():
        return []

    packs = []
    git_available = True
    try:
        subprocess.run(["git", "--version"], capture_output=True, timeout=3)
    except Exception:
        git_available = False

    for domain_dir in sorted(DOMAIN_PACKS_DIR.iterdir()):
        if not domain_dir.is_dir():
            continue
        name = domain_dir.name
        mtime = domain_dir.stat().st_mtime
        created_at = datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()

        builtin = _is_builtin(name) if git_available else False

        try:
            from src.enrichment.rules_loader import EnrichmentRulesLoader
            loader = EnrichmentRulesLoader(name)
            enrich_cols = loader.enrichment_column_names
            safety_cols = loader.safety_field_names()
        except Exception:
            enrich_cols = []
            safety_cols = []

        packs.append({
            "domain": name,
            "type": "built-in" if builtin else "user-created",
            "created_at": created_at,
            "enrichment_fields": enrich_cols,
            "safety_fields": safety_cols,
            "git_available": git_available,
        })

    return packs


# ---------------------------------------------------------------------------
# Block sequence resolver (T022)
# ---------------------------------------------------------------------------

def _resolve_block_sequence(block_sequence_yaml: str, domain: str) -> tuple[list[str], list[str]]:
    """Parse block_sequence.yaml and return (ordered_block_names, unknown_names).

    Expands __generated__ and dedup_stage aliases.
    """
    try:
        data = yaml.safe_load(block_sequence_yaml)
    except yaml.YAMLError as exc:
        return [], [f"YAML parse error: {exc}"]

    raw_sequence = data.get("sequence", data.get("block_sequence", []))
    if not isinstance(raw_sequence, list):
        return [], ["'sequence' key is not a list"]

    expanded = []
    for name in raw_sequence:
        if name == "__generated__":
            expanded.append("[DynamicMappingBlock]")
        elif name == "dedup_stage":
            expanded.extend(["fuzzy_deduplicate", "column_wise_merge", "golden_record_select"])
        else:
            expanded.append(name)

    # Check unknown blocks against registry
    unknown: list[str] = []
    try:
        from src.registry.block_registry import BlockRegistry
        registry = BlockRegistry.instance()
        known = set(registry.list_blocks())
        # Add well-known sentinels and stage names
        known.update({
            "dq_score_pre", "dq_score_post", "schema_enforce",
            "__generated__", "dedup_stage",
            "fuzzy_deduplicate", "column_wise_merge", "golden_record_select",
            "strip_whitespace", "lowercase_brand", "remove_noise_words",
            "strip_punctuation", "llm_enrich",
        })
        for name in raw_sequence:
            if name not in known:
                unknown.append(name)
    except Exception:
        pass

    return expanded, unknown


# ---------------------------------------------------------------------------
# Enrichment rules validator (T023)
# ---------------------------------------------------------------------------

def _validate_enrichment_rules(enrichment_yaml: str) -> tuple[list[str], list[str]]:
    """Return (errors, warnings) for an enrichment_rules.yaml string."""
    errors: list[str] = []
    warnings: list[str] = []

    try:
        data = yaml.safe_load(enrichment_yaml)
    except yaml.YAMLError as exc:
        return [f"YAML parse error: {exc}"], []

    if not isinstance(data, dict):
        return ["Top-level value is not a mapping"], []

    if "domain" not in data:
        errors.append("Missing required 'domain' key")

    if "fields" not in data:
        errors.append("Missing required 'fields' key")
        return errors, warnings

    fields = data.get("fields", [])
    if not fields:
        warnings.append("No fields declared")
        return errors, warnings

    # Collect safety field names (deterministic strategy)
    safety_names = {
        f["name"] for f in fields
        if isinstance(f, dict) and f.get("strategy") == "deterministic"
    }

    for f in fields:
        if not isinstance(f, dict):
            continue
        name = f.get("name", "<unnamed>")
        strategy = f.get("strategy")
        patterns = f.get("patterns", [])

        if strategy == "llm" and name in safety_names:
            errors.append(
                f"Field '{name}' is declared as LLM strategy but is also in safety fields — "
                "safety fields must use deterministic strategy only"
            )

        if not patterns:
            warnings.append(f"Field '{name}' has no patterns defined")

    return errors, warnings


# ---------------------------------------------------------------------------
# Writability check
# ---------------------------------------------------------------------------

def _check_writability() -> None:
    """Render an error banner if domain_packs/ is not writable."""
    try:
        import streamlit as st
    except ImportError:
        return
    if not os.access(str(DOMAIN_PACKS_DIR), os.W_OK):
        st.error(
            f"`domain_packs/` at `{DOMAIN_PACKS_DIR}` is not writable by this process. "
            "Kit creation and deletion will fail. Check filesystem permissions."
        )


# ---------------------------------------------------------------------------
# Tab renderers
# ---------------------------------------------------------------------------

def _render_generate_tab() -> None:
    try:
        import streamlit as st
    except ImportError:
        return

    st.subheader("Generate Domain Pack")
    st.caption("Enter domain details and provide a sample CSV. The AI will generate three YAML configuration files.")

    # --- Fixture quick-load ---
    st.markdown("**Quick-load a sample fixture**")
    preset_labels = ["— pick a fixture to pre-fill —"] + [p[0] for p in _FIXTURE_PRESETS]
    preset_choice = st.selectbox("Sample fixtures", preset_labels, key="kit_preset_choice")

    fixture_csv_content = ""
    if preset_choice != preset_labels[0]:
        preset = next(p for p in _FIXTURE_PRESETS if p[0] == preset_choice)
        fixture_path = FIXTURES_DIR / preset[1]
        if fixture_path.exists():
            fixture_csv_content = fixture_path.read_text()
            col_load1, col_load2 = st.columns([2, 1])
            with col_load1:
                st.caption(f"Loaded `{preset[1]}` ({fixture_csv_content.count(chr(10))} rows)")
            with col_load2:
                if st.button("Apply to form", key="kit_apply_preset"):
                    st.session_state["kit_domain_name"] = preset[2]
                    st.session_state["kit_description"] = preset[3]
                    st.session_state["kit_fixture_content"] = fixture_csv_content
                    st.rerun()
            with st.expander("Preview CSV", expanded=False):
                preview_lines = fixture_csv_content.splitlines()[:6]
                st.code("\n".join(preview_lines), language="text")
        else:
            st.warning(f"Fixture file not found: `{fixture_path}`")

    st.markdown("---")

    domain_name = st.text_input(
        "Domain name",
        placeholder="healthcare",
        help="Lowercase letters, digits, underscores. Must start with a letter.",
        key="kit_domain_name",
    )

    slug_valid = bool(domain_name and _SLUG_RE.match(domain_name))
    if domain_name and not slug_valid:
        st.error("Domain name must match `[a-z][a-z0-9_]*`")

    description = st.text_area(
        "Domain description",
        placeholder="Healthcare patient discharge records with ICD-10 diagnosis codes and medication lists.",
        key="kit_description",
    )

    uploaded = st.file_uploader(
        "Sample CSV — upload your own (overrides fixture above)",
        type=["csv"],
        key="kit_csv",
    )

    csv_content = ""
    if uploaded is not None:
        csv_content = uploaded.read().decode("utf-8", errors="replace")
    elif st.session_state.get("kit_fixture_content"):
        csv_content = st.session_state["kit_fixture_content"]

    if csv_content:
        line_count = csv_content.count("\n")
        cols = csv_content.splitlines()[0].count(",") + 1 if csv_content else 0
        st.caption(f"CSV ready: {line_count} rows, {cols} columns")

    can_generate = slug_valid and description.strip() and csv_content

    if st.button("Generate", disabled=not can_generate, key="kit_generate_btn"):
        from src.ui.kit_generator import generate_domain_kit
        with st.spinner("Generating domain pack… (LLM call, ~15–30 seconds)"):
            try:
                pack = generate_domain_kit(domain_name, description, csv_content)
                st.session_state["pack_gen"] = pack
                st.session_state["kit_domain_committed"] = False
            except Exception as exc:
                st.error(f"Generation failed: {exc}")
                if st.button("Retry", key="kit_retry_btn"):
                    st.rerun()

    pack = st.session_state.get("pack_gen")
    if not pack:
        return

    st.markdown("---")
    st.subheader("Review Generated Files")
    st.caption("Edit the files below before committing. Each file is validated on save.")

    file_keys = ["enrichment_rules.yaml", "prompt_examples.yaml", "block_sequence.yaml"]
    edited: dict[str, str] = {}
    file_errors: dict[str, str] = {}

    for fname in file_keys:
        raw = pack.get(fname, "")
        if raw.startswith('{"error"'):
            try:
                err_msg = json.loads(raw).get("error", raw)
            except Exception:
                err_msg = raw
            st.error(f"**{fname}**: {err_msg}")
            if st.button(f"Retry {fname}", key=f"retry_{fname}"):
                st.rerun()
            file_errors[fname] = err_msg
            continue

        edited_val = st.text_area(
            fname,
            value=raw,
            height=300,
            key=f"kit_edit_{fname}",
        )
        edited[fname] = edited_val

        # Inline YAML validation
        try:
            yaml.safe_load(edited_val)
            st.success(f"✓ {fname} — valid YAML", icon=None)
        except yaml.YAMLError as exc:
            st.warning(f"{fname} — YAML syntax error: {exc}")
            file_errors[fname] = str(exc)

    validated = not file_errors and len(edited) == len(file_keys)

    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("Validate", key="kit_validate_btn"):
            all_ok = True
            for fname, content in edited.items():
                errs, warns = _validate_enrichment_rules(content) if fname == "enrichment_rules.yaml" else ([], [])
                for e in errs:
                    st.error(f"{fname}: {e}")
                    all_ok = False
                for w in warns:
                    st.warning(f"{fname}: {w}")
            if "block_sequence.yaml" in edited:
                _, unknown = _resolve_block_sequence(edited["block_sequence.yaml"], domain_name)
                for u in unknown:
                    st.warning(f"block_sequence.yaml: unknown block '{u}'")
            if all_ok:
                st.success("Validation passed")

    with col2:
        if st.button("Commit", disabled=not validated, key="kit_commit_btn"):
            domain_dir = DOMAIN_PACKS_DIR / domain_name
            try:
                domain_dir.mkdir(parents=True, exist_ok=True)
                for fname, content in edited.items():
                    (domain_dir / fname).write_text(content)
                _append_audit(domain_name, "generate", "success", f"committed {list(edited.keys())}")
                st.session_state["kit_domain_committed"] = True
                st.success(f"Domain '{domain_name}' committed to `domain_packs/{domain_name}/`")
                if st.button("Run Pipeline with this domain", key="kit_run_pipeline_btn"):
                    st.session_state["app_mode"] = "Pipeline"
                    st.session_state["domain"] = domain_name
                    st.rerun()
            except Exception as exc:
                _append_audit(domain_name, "generate", "error", str(exc))
                st.error(f"Commit failed: {exc}")


def _render_scaffold_tab() -> None:
    try:
        import streamlit as st
    except ImportError:
        return

    st.subheader("Custom Block Scaffold")
    st.caption("Describe what to extract and the AI generates a Python `Block` subclass scaffold for download.")

    domains = [d.name for d in sorted(DOMAIN_PACKS_DIR.iterdir()) if d.is_dir()] if DOMAIN_PACKS_DIR.exists() else []
    selected_domain = st.selectbox("Domain", domains or ["<none>"], key="scaffold_domain")

    extraction_description = st.text_area(
        "Describe what to extract",
        placeholder="Extract ICD-10 codes from the diagnosis_text column using regex patterns.",
        height=120,
        key="scaffold_description",
    )

    can_generate = bool(extraction_description.strip() and selected_domain and selected_domain != "<none>")

    if st.button("Generate Block", disabled=not can_generate, key="scaffold_generate_btn"):
        from src.ui.block_scaffolder import generate_block_scaffold
        with st.spinner("Generating block scaffold…"):
            try:
                source, syntax_valid = generate_block_scaffold(selected_domain, extraction_description)
                st.session_state["scaffold"] = {"source": source, "syntax_valid": syntax_valid}
            except Exception as exc:
                st.error(f"Scaffold generation failed: {exc}")

    scaffold = st.session_state.get("scaffold")
    if not scaffold:
        return

    source = scaffold.get("source", "")
    syntax_valid = scaffold.get("syntax_valid", False)

    st.markdown("---")
    if syntax_valid:
        st.success("✓ Syntax valid")
    else:
        st.error("✗ Syntax error in generated code (shown below)")

    st.code(source, language="python")

    st.warning(
        "**Security notice**: This file will execute on the server when placed in "
        f"`domain_packs/{selected_domain}/custom_blocks/`. "
        "Review it carefully before deployment."
    )

    ack = st.checkbox(
        "I understand this file will execute on the server when placed in custom_blocks/",
        key="scaffold_ack",
    )
    st.session_state["scaffold_ack"] = ack

    download_enabled = syntax_valid and ack
    st.download_button(
        "Download scaffold.py",
        data=source.encode("utf-8"),
        file_name=f"{selected_domain}_block.py",
        mime="text/x-python",
        disabled=not download_enabled,
        key="scaffold_download_btn",
    )


def _render_preview_tab() -> None:
    try:
        import streamlit as st
    except ImportError:
        return

    st.subheader("Preview / Validate Domain Pack")
    st.caption("Resolve block execution order and validate enrichment rules without writing to disk.")

    domains = [d.name for d in sorted(DOMAIN_PACKS_DIR.iterdir()) if d.is_dir()] if DOMAIN_PACKS_DIR.exists() else []
    selected_domain = st.selectbox("Domain", domains or ["<none>"], key="preview_domain")

    paste_mode = st.checkbox("Paste YAML directly (instead of loading from disk)", key="preview_paste_mode")

    bs_yaml = ""
    er_yaml = ""

    if paste_mode:
        bs_yaml = st.text_area("block_sequence.yaml content", height=200, key="preview_bs_yaml")
        er_yaml = st.text_area("enrichment_rules.yaml content", height=200, key="preview_er_yaml")
    else:
        if selected_domain and selected_domain != "<none>":
            bs_path = DOMAIN_PACKS_DIR / selected_domain / "block_sequence.yaml"
            er_path = DOMAIN_PACKS_DIR / selected_domain / "enrichment_rules.yaml"
            bs_yaml = bs_path.read_text() if bs_path.exists() else ""
            er_yaml = er_path.read_text() if er_path.exists() else ""

    if st.button("Preview", key="preview_btn"):
        if not bs_yaml and not er_yaml:
            st.warning("No YAML content to preview. Select a domain or paste YAML above.")
            return

        domain_label = selected_domain if not paste_mode else "(pasted)"

        st.markdown("---")

        if bs_yaml:
            st.markdown("### Block Execution Order")
            resolved, unknown = _resolve_block_sequence(bs_yaml, domain_label)
            if resolved:
                rows = [{"#": i + 1, "Block": name} for i, name in enumerate(resolved)]
                st.table(rows)
            for u in unknown:
                st.warning(f"Unknown block: `{u}`")

        if er_yaml:
            st.markdown("### Enrichment Field Summary")
            try:
                er_data = yaml.safe_load(er_yaml)
                st.json(er_data)
            except yaml.YAMLError as exc:
                st.error(f"enrichment_rules.yaml parse error: {exc}")

            errs, warns = _validate_enrichment_rules(er_yaml)
            for e in errs:
                st.error(e)
            for w in warns:
                st.warning(w)
            if not errs and not warns:
                st.success("enrichment_rules.yaml validation passed")


def _render_manage_tab() -> None:
    try:
        import streamlit as st
    except ImportError:
        return

    st.subheader("Manage Domain Packs")
    packs = _list_domain_packs()

    if not packs:
        st.info("No domain packs found in `domain_packs/`.")
        return

    # Warn if git not available for built-in detection
    if packs and not packs[0].get("git_available", True):
        st.warning(
            "git is not available — all domains classified as 'user-created'. "
            "Install git to enable built-in domain detection."
        )

    # Summary table
    table_data = [
        {
            "Domain": p["domain"],
            "Type": p["type"],
            "Created": p["created_at"][:10],
            "Enrichment Fields": ", ".join(p["enrichment_fields"]) or "(none)",
            "Safety Fields": ", ".join(p["safety_fields"]) or "(none)",
        }
        for p in packs
    ]
    st.dataframe(table_data, use_container_width=True)

    st.markdown("---")
    st.subheader("Actions")

    selected_for_audit = st.selectbox(
        "Select domain for details", [p["domain"] for p in packs], key="manage_selected"
    )

    for pack in packs:
        name = pack["domain"]
        if pack["type"] == "built-in":
            st.caption(f"**{name}**: Protected — built-in domain pack")
        else:
            confirm_key = f"confirm_delete_{name}"
            if st.session_state.get(confirm_key):
                st.error(f"Are you sure you want to delete `{name}`? This cannot be undone.")
                col_yes, col_no = st.columns([1, 1])
                with col_yes:
                    if st.button(f"Yes, delete {name}", key=f"confirm_yes_{name}"):
                        _append_audit(name, "delete", "pending", "pre-rmtree")
                        try:
                            shutil.rmtree(str(DOMAIN_PACKS_DIR / name))
                            _append_audit(name, "delete", "success", "rmtree completed")
                            st.session_state.pop(confirm_key, None)
                            st.rerun()
                        except Exception as exc:
                            _append_audit(name, "delete", "error", str(exc))
                            st.error(f"Delete failed: {exc}")
                with col_no:
                    if st.button(f"Cancel", key=f"confirm_no_{name}"):
                        st.session_state.pop(confirm_key, None)
                        st.rerun()
            else:
                if st.button(f"Delete {name}", key=f"delete_{name}"):
                    st.session_state[confirm_key] = True
                    st.rerun()

    # Audit log for selected domain
    if selected_for_audit:
        st.markdown("---")
        st.subheader(f"Audit Log: {selected_for_audit}")
        entries = _load_audit_log(selected_for_audit)
        if entries:
            st.table(entries)
        else:
            st.caption("No audit entries yet.")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def render_domain_kits_page() -> None:
    try:
        import streamlit as st
    except ImportError:
        logger.error("streamlit not installed — cannot render domain kits page")
        return

    st.title("Domain Packs")
    _check_writability()

    tab1, tab2, tab3, tab4 = st.tabs(["Generate Pack", "Block Scaffold", "Preview / Validate", "Manage Packs"])

    with tab1:
        _render_generate_tab()
    with tab2:
        _render_scaffold_tab()
    with tab3:
        _render_preview_tab()
    with tab4:
        _render_manage_tab()
