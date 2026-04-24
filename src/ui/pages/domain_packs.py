"""Domain Packs page — browse, inspect, and generate domain packs."""
from __future__ import annotations
import json
from pathlib import Path
import streamlit as st

PROJECT_ROOT  = Path(__file__).resolve().parent.parent.parent.parent
SCHEMAS_DIR   = PROJECT_ROOT / "config" / "schemas"
GENERATED_DIR = PROJECT_ROOT / "src" / "blocks" / "generated"
PACKS_DIR     = PROJECT_ROOT / "domain_packs"
FIXTURES_DIR  = PROJECT_ROOT / "tests" / "fixtures"

# ── domain metadata ──────────────────────────────────────────────────────────

_META = {
    "nutrition":       ("Food catalog", "Allergens, dietary tags, product categories"),
    "safety":          ("FDA recalls", "Hazard class, recall reason, distribution"),
    "pricing":         ("Retail pricing", "Margin, cost basis, promotions"),
    "pharma":          ("Pharma registry", "NDC codes, active ingredients, FDA approval"),
    "healthcare_test": ("Healthcare", "ICD-10 codes, medications, discharge records"),
    "retail_inventory":("Retail inventory", "SKU data, store-level inventory"),
    "finance":         ("Finance", "Transactions, cost reporting"),
    "manufacturing":   ("Manufacturing", "Batch records, QA metrics"),
}

_GENERATE_PRESETS = [
    {
        "label":       "nutrition — food product catalog",
        "fixture":     "nutrition_sample.csv",
        "domain":      "nutrition",
        "description": "Branded food products with ingredient lists, allergens, dietary tags, and food category classification.",
    },
    {
        "label":       "healthcare_test — patient discharge records",
        "fixture":     "healthcare_sample.csv",
        "domain":      "healthcare_test",
        "description": "Patient discharge records with ICD-10 diagnosis codes, medication lists, and clinical procedures.",
    },
    {
        "label":       "pharma — pharmaceutical drug registry",
        "fixture":     "pharma_sample.csv",
        "domain":      "pharma",
        "description": "Pharmaceutical products with NDC codes, active ingredients, dosage forms, and FDA approval status.",
    },
    {
        "label":       "fda_recalls — food safety recall notices",
        "fixture":     "fda_recalls_sample.csv",
        "domain":      "fda_recalls",
        "description": "FDA food recall notices with recall classification, reason for recall, and distribution pattern.",
    },
]


# ── helpers ──────────────────────────────────────────────────────────────────

def _all_domains() -> list[str]:
    """Union of domain_packs/ dirs + config/schemas/ stems, deduped."""
    pack_dirs  = {d.name for d in PACKS_DIR.iterdir() if d.is_dir()} if PACKS_DIR.exists() else set()
    schema_stems = {p.stem.replace("_schema", "") for p in SCHEMAS_DIR.glob("*_schema.json")}
    ordered = []
    for d in sorted(pack_dirs | schema_stems):
        ordered.append(d)
    return ordered


def _pack_files(domain: str) -> dict[str, str]:
    """Return {filename: content} for the 3 pack YAMLs that exist."""
    pack_dir = PACKS_DIR / domain
    result = {}
    for fname in ("enrichment_rules.yaml", "prompt_examples.yaml", "block_sequence.yaml"):
        p = pack_dir / fname
        if p.exists():
            result[fname] = p.read_text()
    return result


def _schema_props(domain: str) -> tuple[dict, list]:
    """Return (properties_dict, required_list) from config/schemas/<domain>_schema.json."""
    path = SCHEMAS_DIR / f"{domain}_schema.json"
    try:
        data = json.loads(path.read_text())
        # Schema format: {"columns": {col: {"type": ..., "required": bool}}, "dq_weights": {...}}
        cols = data.get("columns", data.get("properties", {}))
        required = [c for c, v in cols.items() if v.get("required") is True]
        return cols, required
    except Exception:
        return {}, []


def _dynamic_mappings(domain: str) -> list[Path]:
    gen_dir = GENERATED_DIR / domain
    return sorted(gen_dir.glob("DYNAMIC_MAPPING_*.yaml")) if gen_dir.exists() else []


def _chip_row(domains: list[str], selected: str) -> str:
    """Render a read-only chip row (selection handled via st.button below)."""
    items = ""
    for d in domains:
        active = "background:var(--accent);color:#fff;" if d == selected else \
                 "background:var(--surface);color:var(--text);border:1px solid var(--border);"
        items += (
            f'<span style="{active}border-radius:20px;padding:4px 14px;'
            f'font-size:13px;font-weight:600;margin:2px;display:inline-block">{d}</span>'
        )
    return f'<div style="display:flex;flex-wrap:wrap;gap:4px;margin-bottom:8px">{items}</div>'


# ── page ─────────────────────────────────────────────────────────────────────

def render_domain_packs():
    st.markdown("""
    <div class="page-header">
      <div>
        <div class="page-title">Domain Packs</div>
        <div class="page-subtitle">Browse enrichment rules, prompt examples, and block sequences per domain</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    domains = _all_domains()

    # ── domain selector strip ────────────────────────────────────────────────
    if "dp_selected" not in st.session_state:
        st.session_state.dp_selected = domains[0] if domains else "nutrition"

    selected = st.session_state.dp_selected
    st.markdown(_chip_row(domains, selected), unsafe_allow_html=True)

    btn_cols = st.columns(min(len(domains), 8))
    for i, d in enumerate(domains[:8]):
        with btn_cols[i]:
            if st.button(d, key=f"dpbtn_{d}", use_container_width=True):
                st.session_state.dp_selected = d
                st.rerun()

    selected = st.session_state.dp_selected
    short, long_desc = _META.get(selected, ("", ""))

    st.markdown(
        f'<div style="margin:4px 0 16px;font-size:14px;color:var(--text-muted)">'
        f'<strong>{short}</strong>{" — " + long_desc if long_desc else ""}</div>',
        unsafe_allow_html=True,
    )

    # ── tabs ─────────────────────────────────────────────────────────────────
    tab_overview, tab_files, tab_schema, tab_dynamic, tab_generate = st.tabs(
        ["Overview", "Pack Files", "Unified Schema", "Dynamic Mappings", "Generate New"]
    )

    # ── Tab: Overview ────────────────────────────────────────────────────────
    with tab_overview:
        cols = st.columns(3)
        for i, d in enumerate(domains):
            props, _ = _schema_props(d)
            yamls     = _dynamic_mappings(d)
            pack_files = _pack_files(d)
            short_d, desc_d = _META.get(d, ("", ""))
            is_sel = d == selected

            border = "border:2px solid var(--accent);" if is_sel else "border:1px solid var(--border);"
            with cols[i % 3]:
                st.markdown(f"""
                <div class="card" style="{border}cursor:pointer">
                  <div style="font-size:15px;font-weight:700;color:var(--text);margin-bottom:3px">{d}</div>
                  <div style="font-size:12px;color:var(--text-muted);margin-bottom:10px">{desc_d}</div>
                  <div style="display:flex;gap:6px;flex-wrap:wrap">
                    <span class="badge info">{len(props)} schema fields</span>
                    <span class="badge success">{len(pack_files)} pack files</span>
                    <span class="badge purple">{len(yamls)} mappings</span>
                  </div>
                </div>""", unsafe_allow_html=True)
                if st.button(f"Select {d}", key=f"ov_sel_{d}", use_container_width=True):
                    st.session_state.dp_selected = d
                    st.rerun()

    # ── Tab: Pack Files ───────────────────────────────────────────────────────
    with tab_files:
        pack_files = _pack_files(selected)

        if not pack_files:
            st.markdown(
                f'<div class="alert orange">No pack files found in <code>domain_packs/{selected}/</code>. '
                f'Use the Generate New tab to create them.</div>',
                unsafe_allow_html=True,
            )
        else:
            file_tabs = st.tabs([
                f"enrichment_rules.yaml{'  ✓' if 'enrichment_rules.yaml' in pack_files else ''}",
                f"prompt_examples.yaml{'  ✓' if 'prompt_examples.yaml' in pack_files else ''}",
                f"block_sequence.yaml{'  ✓' if 'block_sequence.yaml' in pack_files else ''}",
            ])

            for ft, fname in zip(file_tabs, ("enrichment_rules.yaml", "prompt_examples.yaml", "block_sequence.yaml")):
                with ft:
                    if fname not in pack_files:
                        st.markdown(
                            f'<div class="alert orange">File not found: '
                            f'<code>domain_packs/{selected}/{fname}</code></div>',
                            unsafe_allow_html=True,
                        )
                        continue

                    content = pack_files[fname]

                    # Header row: file path + line count
                    lines = content.count("\n") + 1
                    st.markdown(
                        f'<div style="display:flex;justify-content:space-between;align-items:center;'
                        f'margin-bottom:8px">'
                        f'<span class="mono tc-dim" style="font-size:12px">'
                        f'domain_packs/{selected}/{fname}</span>'
                        f'<span class="badge info">{lines} lines</span></div>',
                        unsafe_allow_html=True,
                    )

                    # Enrichment rules: parsed summary + full YAML
                    if fname == "enrichment_rules.yaml":
                        _render_enrichment_summary(content)

                    # Block sequence: visual block chips + full YAML
                    elif fname == "block_sequence.yaml":
                        _render_sequence_summary(content)

                    # Full YAML (all files)
                    with st.expander("Raw YAML", expanded=(fname == "prompt_examples.yaml")):
                        st.code(content, language="yaml")

    # ── Tab: Unified Schema ───────────────────────────────────────────────────
    with tab_schema:
        props, req = _schema_props(selected)
        if not props:
            st.markdown(
                f'<div class="alert orange">No schema file: '
                f'<code>config/schemas/{selected}_schema.json</code></div>',
                unsafe_allow_html=True,
            )
        else:
            req_set = set(req)
            rows_html = ""
            for col_name, col_def in props.items():
                dtype  = col_def.get("type", "string")
                alias  = col_def.get("enrichment_alias", "")
                note   = f'enrichment alias: {alias}' if alias else ""
                r_badge = '<span class="badge success">required</span>' \
                          if col_name in req_set else '<span class="badge info">optional</span>'
                rows_html += (
                    f'<tr><td><span class="mono">{col_name}</span></td>'
                    f'<td><span class="badge purple">{dtype}</span></td>'
                    f'<td>{r_badge}</td>'
                    f'<td class="tc-dim" style="font-size:12px">{note}</td></tr>'
                )
            st.markdown(f"""
            <div class="card">
              <div class="card-title">{selected} schema — {len(props)} fields</div>
              <table class="data-table">
                <thead><tr><th>Column</th><th>Type</th><th>Required</th><th>Description</th></tr></thead>
                <tbody>{rows_html}</tbody>
              </table>
            </div>""", unsafe_allow_html=True)

    # ── Tab: Dynamic Mappings ─────────────────────────────────────────────────
    with tab_dynamic:
        yamls = _dynamic_mappings(selected)
        if not yamls:
            st.markdown(
                f'<div class="alert orange">No generated mappings for <strong>{selected}</strong>. '
                f'Run the pipeline with <code>--domain {selected}</code> to generate one.</div>',
                unsafe_allow_html=True,
            )
        else:
            names = [p.stem.replace("DYNAMIC_MAPPING_", "") for p in yamls]
            sel_src = st.selectbox("Source mapping", names, key="dp_dyn_src")
            yaml_path = yamls[names.index(sel_src)]
            content = yaml_path.read_text()
            st.markdown(
                f'<span class="mono tc-dim" style="font-size:12px">{yaml_path.relative_to(PROJECT_ROOT)}</span>',
                unsafe_allow_html=True,
            )
            st.code(content, language="yaml")

    # ── Tab: Generate New ─────────────────────────────────────────────────────
    with tab_generate:
        _render_generate_tab()


# ── enrichment rules summary ─────────────────────────────────────────────────

def _render_enrichment_summary(yaml_text: str):
    try:
        import yaml as _yaml
        data = _yaml.safe_load(yaml_text) or {}
    except Exception:
        return

    fields = data.get("fields", [])
    if not fields:
        return

    st.markdown(
        f'<div style="margin-bottom:10px">'
        f'<span class="badge purple">{len(fields)} fields</span>&nbsp;'
        f'<span class="badge info">{data.get("domain","")}</span>&nbsp;'
        f'<span class="tc-dim" style="font-size:12px">text_columns: '
        f'{", ".join(data.get("text_columns", []))}</span></div>',
        unsafe_allow_html=True,
    )

    rows_html = ""
    for f in fields:
        name     = f.get("name", "")
        strategy = f.get("strategy", "")
        out_type = f.get("output_type", "")
        n_pat    = len(f.get("patterns", []))
        s_badge  = "error" if strategy == "deterministic" else "running"
        rows_html += (
            f'<tr><td><span class="mono">{name}</span></td>'
            f'<td><span class="badge {s_badge}">{strategy}</span></td>'
            f'<td><span class="badge info">{out_type}</span></td>'
            f'<td style="font-size:13px">{n_pat} patterns</td></tr>'
        )

    st.markdown(f"""
    <div class="card" style="margin-bottom:10px">
      <div class="card-title">Enrichment Fields</div>
      <table class="data-table" style="font-size:13px">
        <thead><tr><th>Field</th><th>Strategy</th><th>Output</th><th>Patterns</th></tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>""", unsafe_allow_html=True)

    # Pattern detail per field
    with st.expander("Pattern detail per field"):
        for f in fields:
            name = f.get("name", "")
            patterns = f.get("patterns", [])
            if patterns:
                pat_html = "".join(
                    f'<div style="font-size:12px;font-family:var(--mono);color:var(--text-muted);'
                    f'padding:2px 0">'
                    f'<span style="color:var(--accent)">{p.get("label","")}</span>  '
                    f'{p.get("regex","")}</div>'
                    for p in patterns
                )
                st.markdown(
                    f'<div style="margin-bottom:8px"><strong style="font-size:13px">{name}</strong>'
                    f'<div style="margin-top:4px;padding:6px 10px;background:var(--surface);'
                    f'border-radius:4px;border:1px solid var(--border)">{pat_html}</div></div>',
                    unsafe_allow_html=True,
                )


# ── block sequence summary ───────────────────────────────────────────────────

def _render_sequence_summary(yaml_text: str):
    try:
        import yaml as _yaml
        data = _yaml.safe_load(yaml_text) or {}
    except Exception:
        return

    def _chips(seq: list[str]) -> str:
        html = '<div style="display:flex;flex-wrap:wrap;gap:6px;margin:6px 0 10px">'
        for b in seq:
            if b in ("dq_score_pre", "dq_score_post"):
                cls = "badge success"
            elif b == "__generated__":
                cls = "badge purple"
            elif b in ("fuzzy_deduplicate", "column_wise_merge", "golden_record_select"):
                cls = "badge warning"
            elif b in ("llm_enrich", "extract_allergens"):
                cls = "badge running"
            else:
                cls = "badge info"
            html += f'<span class="{cls}" style="font-size:12px">{b}</span>'
        html += "</div>"
        return html

    for seq_key, label in (
        ("sequence", "Full pipeline"),
        ("silver_sequence", "Silver mode"),
        ("gold_sequence", "Gold mode"),
    ):
        seq = data.get(seq_key)
        if seq:
            st.markdown(
                f'<div style="font-size:13px;font-weight:600;color:var(--text-muted);'
                f'margin-top:8px;text-transform:uppercase;letter-spacing:.05em">{label} '
                f'<span class="badge info">{len(seq)} blocks</span></div>',
                unsafe_allow_html=True,
            )
            st.markdown(_chips(seq), unsafe_allow_html=True)


# ── generate tab (wired to real domain_kits agent) ───────────────────────────

def _render_generate_tab():
    st.markdown(
        '<div class="card-title" style="font-size:15px;margin-bottom:4px">Generate Domain Pack</div>'
        '<div style="font-size:13px;color:var(--text-muted);margin-bottom:14px">'
        'LLM agent produces enrichment_rules.yaml, prompt_examples.yaml, and block_sequence.yaml '
        'in 5 steps with HITL approval before writing to disk.</div>',
        unsafe_allow_html=True,
    )

    # Pre-fill presets
    st.markdown("**Quick-load a preset**")
    preset_labels = ["-- pick a preset to pre-fill --"] + [p["label"] for p in _GENERATE_PRESETS]
    chosen = st.selectbox("Preset", preset_labels, key="dp_gen_preset")

    if chosen != preset_labels[0]:
        preset = next(p for p in _GENERATE_PRESETS if p["label"] == chosen)
        col_a, col_b = st.columns([2, 1])
        with col_a:
            fixture_path = FIXTURES_DIR / preset["fixture"]
            if fixture_path.exists():
                content_preview = fixture_path.read_text()
                rows = content_preview.count("\n")
                cols_n = content_preview.splitlines()[0].count(",") + 1 if content_preview else 0
                st.caption(f"Fixture: `{preset['fixture']}` — {rows} rows, {cols_n} columns")
                with st.expander("Preview CSV"):
                    st.code("\n".join(content_preview.splitlines()[:6]), language="text")
            else:
                st.warning(f"Fixture not found: `{fixture_path}`")
                content_preview = ""
        with col_b:
            if st.button("Apply to form", key="dp_gen_apply", use_container_width=True):
                st.session_state["dp_gen_domain"]      = preset["domain"]
                st.session_state["dp_gen_description"] = preset["description"]
                st.session_state["dp_gen_csv"]         = content_preview
                st.rerun()

    st.markdown("---")

    import re as _re
    _SLUG = _re.compile(r"^[a-z][a-z0-9_]*$")

    domain_name = st.text_input(
        "Domain name",
        placeholder="e.g. cosmetics",
        key="dp_gen_domain",
        help="Lowercase, starts with letter. Will create domain_packs/<name>/",
    )
    slug_ok = bool(domain_name and _SLUG.match(domain_name))
    if domain_name and not slug_ok:
        st.error("Must match `[a-z][a-z0-9_]*`")

    description = st.text_area(
        "Domain description",
        placeholder="Describe the data domain and key fields to extract.",
        key="dp_gen_description",
        height=80,
    )

    uploaded = st.file_uploader("Sample CSV (upload overrides preset)", type=["csv"], key="dp_gen_upload")
    csv_content: str = ""
    if uploaded:
        csv_content = uploaded.read().decode("utf-8", errors="replace")
        st.session_state["dp_gen_csv"] = csv_content
    else:
        csv_content = st.session_state.get("dp_gen_csv", "")

    if csv_content:
        n_rows = csv_content.count("\n")
        n_cols = csv_content.splitlines()[0].count(",") + 1 if csv_content else 0
        st.caption(f"CSV ready: {n_rows} rows, {n_cols} columns")

    can_generate = slug_ok and description.strip() and csv_content

    try:
        from src.agents.domain_kit_graph import DomainKitState, run_kit_step
        _kit_available = True
    except Exception:
        _kit_available = False

    if not _kit_available:
        st.warning(
            "Kit generator agent unavailable. Run via CLI instead: "
            f"`poetry run python -m src.pipeline.cli --source <csv> --domain {domain_name or '<domain>'}`"
        )
        return

    kit_state: dict = st.session_state.get("dp_kit_state", {})

    if st.button("Generate Domain Pack", type="primary", disabled=not can_generate, key="dp_gen_btn"):
        kit_state = DomainKitState(
            domain_name=domain_name,
            description=description,
            csv_content=csv_content,
            retry_count=0,
            validation_errors=[],
        )
        with st.spinner("Step 1/5 - Analysing CSV..."):
            kit_state = run_kit_step("analyze_csv", kit_state)
        if kit_state.get("error"):
            st.error(f"CSV analysis failed: {kit_state['error']}")
            st.session_state["dp_kit_state"] = kit_state
            return

        with st.spinner("Step 2/5 - Generating enrichment rules..."):
            kit_state = run_kit_step("generate_enrichment_rules", kit_state)
        if kit_state.get("error"):
            st.error(f"Enrichment rules failed: {kit_state['error']}")
            st.session_state["dp_kit_state"] = kit_state
            return

        with st.spinner("Step 3/5 - Validating enrichment rules..."):
            kit_state = run_kit_step("validate_enrichment_rules", kit_state)

        retries = 0
        while kit_state.get("validation_errors") and retries < 2:
            retries += 1
            with st.spinner(f"Step 3/5 - Revising enrichment rules (attempt {retries}/2)..."):
                kit_state = run_kit_step("revise_enrichment_rules", kit_state)
                kit_state = run_kit_step("validate_enrichment_rules", kit_state)

        with st.spinner("Step 4/5 - Generating prompt examples..."):
            kit_state = run_kit_step("generate_prompt_examples", kit_state)

        with st.spinner("Step 5/5 - Generating block sequence..."):
            kit_state = run_kit_step("generate_block_sequence", kit_state)

        kit_state = run_kit_step("hitl_review", kit_state)
        st.session_state["dp_kit_state"] = kit_state
        st.rerun()

    # ── HITL review ───────────────────────────────────────────────────────────
    if not kit_state.get("pending_review"):
        return

    st.markdown("---")
    st.markdown('<div class="card-title">Review Generated Files</div>', unsafe_allow_html=True)

    errs = kit_state.get("validation_errors", [])
    if errs:
        st.warning("Enrichment rules have unresolved validation issues — review before approving.")
        for e in errs:
            st.error(e)

    file_map = {
        "enrichment_rules.yaml": kit_state.get("enrichment_rules_yaml", ""),
        "prompt_examples.yaml":  kit_state.get("prompt_examples_yaml", ""),
        "block_sequence.yaml":   kit_state.get("block_sequence_yaml", ""),
    }

    import yaml as _yaml
    user_edits: dict[str, str] = {}
    yaml_errors: list[str] = []
    rev_tabs = st.tabs(list(file_map.keys()))
    for rt, (fname, raw) in zip(rev_tabs, file_map.items()):
        with rt:
            edited = st.text_area(fname, value=raw, height=320, key=f"dp_edit_{fname}", label_visibility="collapsed")
            user_edits[fname] = edited
            try:
                _yaml.safe_load(edited)
            except _yaml.YAMLError as exc:
                st.warning(f"YAML syntax error: {exc}")
                yaml_errors.append(fname)

    if yaml_errors:
        st.error(f"Fix YAML syntax in: {yaml_errors}")

    if st.button("Approve & Save All", type="primary", disabled=bool(yaml_errors), key="dp_approve_btn"):
        kit_state = {**kit_state, "user_edits": user_edits}
        kit_state = run_kit_step("commit_to_disk", kit_state)
        st.session_state["dp_kit_state"] = kit_state

        if kit_state.get("committed"):
            gen_domain = kit_state.get("domain_name", "")
            st.success(f"Domain pack `{gen_domain}` saved to `domain_packs/{gen_domain}/`")
            if st.button("View in Pack Files", key="dp_goto_files"):
                st.session_state.dp_selected = gen_domain
                st.session_state.pop("dp_kit_state", None)
                st.rerun()
        else:
            st.error(f"Commit failed: {kit_state.get('error', 'unknown')}")
