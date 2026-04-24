"""Domain Packs page — browse generated YAML mappings per domain."""
from __future__ import annotations
import json
from pathlib import Path
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SCHEMAS_DIR  = PROJECT_ROOT / "config" / "schemas"
GENERATED_DIR = PROJECT_ROOT / "src" / "blocks" / "generated"

DOMAINS = ["nutrition", "safety", "pricing", "retail", "finance", "manufacturing"]

DOMAIN_ICONS = {
    "nutrition": "🥗", "safety": "🛡", "pricing": "💰",
    "retail": "🛒", "finance": "📈", "manufacturing": "🏭",
}

DOMAIN_DESC = {
    "nutrition":     "Food product catalog — allergens, ingredients, dietary tags",
    "safety":        "FDA recall + safety data — hazard classifications",
    "pricing":       "Retail pricing + promotions — margin and cost basis",
    "retail":        "Store-level SKU data — inventory and product listings",
    "finance":       "Financial transactions and cost reporting",
    "manufacturing": "Plant operations, batch records, QA metrics",
}


def _load_schema(domain: str) -> dict:
    path = SCHEMAS_DIR / f"{domain}_schema.json"
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _load_generated_yamls(domain: str) -> list[Path]:
    gen_dir = GENERATED_DIR / domain
    if not gen_dir.exists():
        return []
    return sorted(gen_dir.glob("DYNAMIC_MAPPING_*.yaml"))


def _highlight_yaml(text: str) -> str:
    import re
    lines = text.splitlines()
    out = []
    for line in lines:
        # key: value
        m = re.match(r'^(\s*)([^#:\s][^:]*?)(:)(.*)', line)
        if m:
            indent, key, colon, val = m.groups()
            val_stripped = val.strip()
            if val_stripped.lower() in ("true", "false", "null"):
                val_html = f' <span class="yaml-bool">{val_stripped}</span>'
            elif re.match(r'^-?\d+(\.\d+)?$', val_stripped):
                val_html = f' <span class="yaml-num">{val_stripped}</span>'
            elif val_stripped.startswith('"') or val_stripped.startswith("'"):
                val_html = f' <span class="yaml-val">{val_stripped}</span>'
            else:
                val_html = f' <span class="yaml-val">{val_stripped}</span>' if val_stripped else ""
            out.append(f'{indent}<span class="yaml-key">{key}</span>{colon}{val_html}')
        else:
            out.append(line)
    return "\n".join(out)


def render_domain_packs():
    st.markdown("""
    <div class="page-header">
      <div>
        <div class="page-title">Domain Packs</div>
        <div class="page-subtitle">Generated YAML transform mappings by domain</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    tabs = st.tabs(["Overview", "Schema Explorer", "Generated YAMLs", "Kit Generator"])

    # ── Tab 0: Overview ───────────────────────────────────────────────────────
    with tabs[0]:
        cols = st.columns(3)
        for i, domain in enumerate(DOMAINS):
            schema = _load_schema(domain)
            yamls  = _load_generated_yamls(domain)
            fields = schema.get("properties", {})
            col = cols[i % 3]
            with col:
                st.markdown(f"""
                <div class="card">
                  <div style="font-size:22px;margin-bottom:6px;">{DOMAIN_ICONS.get(domain,"◈")}</div>
                  <div style="font-size:16px;font-weight:700;color:var(--text);margin-bottom:4px;">{domain.capitalize()}</div>
                  <div style="font-size:13px;color:var(--text-muted);margin-bottom:12px;">{DOMAIN_DESC.get(domain,"")}</div>
                  <div style="display:flex;gap:8px;flex-wrap:wrap;">
                    <span class="badge info">{len(fields)} fields</span>
                    <span class="badge success">{len(yamls)} mappings</span>
                  </div>
                </div>""", unsafe_allow_html=True)

    # ── Tab 1: Schema Explorer ────────────────────────────────────────────────
    with tabs[1]:
        sel_domain = st.selectbox("Domain", DOMAINS, key="schema_domain")
        schema = _load_schema(sel_domain)
        props  = schema.get("properties", {})
        req    = set(schema.get("required", []))

        if props:
            rows_html = ""
            for col_name, col_def in props.items():
                dtype = col_def.get("type", "string")
                desc  = col_def.get("description", "")
                req_badge = '<span class="badge success">required</span>' if col_name in req else '<span class="badge info">optional</span>'
                rows_html += f"""
                <tr>
                  <td><span class="mono">{col_name}</span></td>
                  <td><span class="badge purple">{dtype}</span></td>
                  <td>{req_badge}</td>
                  <td class="tc-dim">{desc}</td>
                </tr>"""
            st.markdown(f"""
            <div class="card">
              <div class="card-title">{sel_domain.capitalize()} Schema — {len(props)} fields</div>
              <table class="data-table">
                <thead><tr><th>Column</th><th>Type</th><th>Required</th><th>Description</th></tr></thead>
                <tbody>{rows_html}</tbody>
              </table>
            </div>""", unsafe_allow_html=True)
        else:
            st.markdown('<div class="alert orange">No schema file found for this domain.</div>', unsafe_allow_html=True)

    # ── Tab 2: Generated YAMLs ────────────────────────────────────────────────
    with tabs[2]:
        sel_domain2 = st.selectbox("Domain", DOMAINS, key="yaml_domain")
        yamls = _load_generated_yamls(sel_domain2)

        if yamls:
            yaml_names = [p.stem.replace("DYNAMIC_MAPPING_", "") for p in yamls]
            sel_source = st.selectbox("Source", yaml_names, key="yaml_source")
            yaml_path  = yamls[yaml_names.index(sel_source)]
            try:
                content = yaml_path.read_text()
                highlighted = _highlight_yaml(content)
                st.markdown(f"""
                <div class="card">
                  <div class="card-title">{yaml_path.name}</div>
                  <div class="yaml-editor"><pre style="margin:0;white-space:pre-wrap;">{highlighted}</pre></div>
                </div>""", unsafe_allow_html=True)
            except Exception as e:
                st.error(str(e))
        else:
            st.markdown(f'<div class="alert orange">No generated YAML mappings found for {sel_domain2}.</div>', unsafe_allow_html=True)

    # ── Tab 3: Kit Generator ──────────────────────────────────────────────────
    with tabs[3]:
        try:
            from src.ui.kit_generator import render_kit_generator
            render_kit_generator()
        except Exception:
            st.markdown("""
            <div class="card">
              <div class="card-title">Kit Generator</div>
              <div style="color:var(--text-muted);font-size:14px;padding:12px 0;">
                Generate a new domain pack by providing a sample CSV and domain configuration.
              </div>
            </div>""", unsafe_allow_html=True)

            col1, col2 = st.columns(2)
            with col1:
                new_domain = st.text_input("New domain name", placeholder="e.g. cosmetics")
                description = st.text_area("Description", height=80)
            with col2:
                uploaded = st.file_uploader("Sample CSV (optional)", type=["csv"])
                target_schema = st.selectbox("Base schema", DOMAINS + ["—"])

            if st.button("Generate Pack", type="primary"):
                if new_domain:
                    st.info(f"Pack generation for '{new_domain}' would invoke Agent 1 on the uploaded sample — run via CLI: `poetry run python -m src.pipeline.cli --source <csv> --domain {new_domain}`")
                else:
                    st.error("Domain name required.")
