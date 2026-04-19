"""Custom HTML components for the ETL pipeline Streamlit UI."""

from __future__ import annotations

import html

_LOG_LEVEL_CSS = {
    "INFO": "log-info",
    "WARNING": "log-warn",
    "WARN": "log-warn",
    "ERROR": "log-error",
    "CRITICAL": "log-error",
    "DEBUG": "log-debug",
}

_PRIMITIVE_BADGE_CLS = {
    "RENAME": "badge-map",
    "CAST": "badge-derivable",
    "FORMAT": "badge-derivable",
    "DELETE": "badge-drop",
    "ADD": "badge-add",
    "SPLIT": "badge-derive",
    "UNIFY": "badge-derivable",
    "DERIVE": "badge-derive",
    "ENRICH_ALIAS": "badge-alias",
}

_ACTION_BADGE = {
    "set_null": '<span class="badge badge-missing">SET NULL</span>',
    "set_default": '<span class="badge badge-map">SET DEFAULT</span>',
    "type_cast": '<span class="badge badge-derivable">TYPE CAST</span>',
    "rename": '<span class="badge badge-map">RENAME</span>',
    "drop_column": '<span class="badge badge-drop">DROP</span>',
    "json_array_extract_multi": '<span class="badge badge-derive">JSON SPLIT</span>',
    "split_column": '<span class="badge badge-derive">SPLIT</span>',
    "coalesce": '<span class="badge badge-derivable">COALESCE</span>',
    "concat_columns": '<span class="badge badge-derivable">CONCAT</span>',
    "value_map": '<span class="badge badge-derivable">VALUE MAP</span>',
    "parse_date": '<span class="badge badge-derivable">PARSE DATE</span>',
    "regex_replace": '<span class="badge badge-derivable">REGEX</span>',
    "regex_extract": '<span class="badge badge-derivable">REGEX</span>',
    "conditional_map": '<span class="badge badge-derive">COND MAP</span>',
    "expression": '<span class="badge badge-derive">EXPR</span>',
    "contains_flag": '<span class="badge badge-derive">FLAG</span>',
    "extract_json_field": '<span class="badge badge-derive">JSON FIELD</span>',
}


def render_step_bar(
    current_step: int, steps: list[str], max_completed: int = -1
) -> str:
    """
    Render the step indicator bar at the top of the page.

    Args:
        current_step: Currently displayed step.
        steps: List of step labels.
        max_completed: Highest completed step (-1 = none). Steps <= max_completed are clickable.
    """
    items = []
    for i, label in enumerate(steps):
        if i < current_step:
            cls = "done"
        elif i == current_step:
            cls = "active"
        else:
            cls = ""

        clickable = i <= max_completed and i != current_step
        if clickable:
            cls += " clickable"

        check = "&#10003;" if i < current_step else str(i + 1)

        # data-step attribute for JS click handling
        data_attr = f'data-step="{i}"' if clickable else ""
        items.append(
            f'<div class="step-item {cls}" {data_attr}>'
            f'<span class="step-num">{check}</span>{label}'
            f"</div>"
        )
    return f'<div class="step-bar">{"".join(items)}</div>'


def render_source_profile(profile: dict) -> str:
    """Render a source schema profile as an HTML table."""
    rows = []
    for col, info in profile.items():
        null_rate = info.get("null_rate", 0)
        null_cls = (
            "null-low"
            if null_rate < 0.1
            else ("null-mid" if null_rate < 0.4 else "null-high")
        )
        samples = ", ".join(str(s)[:50] for s in info.get("sample_values", [])[:3])
        rows.append(
            f"<tr>"
            f'<td class="col-name">{html.escape(col)}</td>'
            f'<td class="col-type">{html.escape(str(info.get("dtype", "")))}</td>'
            f'<td class="{null_cls} col-null">{null_rate:.1%}</td>'
            f"<td>{info.get('unique_count', '')}</td>"
            f'<td class="sample">{html.escape(samples)}</td>'
            f"</tr>"
        )
    return (
        '<table class="profile-table">'
        "<thead><tr><th>Column</th><th>Type</th><th>Null Rate</th><th>Unique</th><th>Samples</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def render_schema_delta(
    source_profile: dict,
    column_mapping: dict,
    gaps: list[dict],
    unified_schema: dict | None = None,
    missing_columns: list[dict] | None = None,
    derivable_gaps: list[dict] | None = None,
    enrichment_columns: list[str] | None = None,
    enrich_alias_ops: list[dict] | None = None,
) -> str:
    """
    Render a side-by-side schema delta table.

    Shows: source column -> unified column, action badge, type, null rate.
    When missing_columns and derivable_gaps are provided, uses the new
    classification badges (MISSING, TYPE_CAST, DERIVE) instead of generic GAP.
    """
    rows = []

    # Mapped columns
    for src_col, uni_col in column_mapping.items():
        src_info = source_profile.get(src_col, {})
        null_rate = src_info.get("null_rate", 0)
        null_cls = (
            "null-low"
            if null_rate < 0.1
            else ("null-mid" if null_rate < 0.4 else "null-high")
        )
        src_type = src_info.get("dtype", "?")
        uni_type = ""
        if unified_schema:
            uni_spec = unified_schema.get("columns", {}).get(uni_col, {})
            uni_type = uni_spec.get("type", "")

        rows.append(
            f"<tr>"
            f'<td class="col-source">{html.escape(src_col)}</td>'
            f"<td>&#8594;</td>"
            f'<td class="col-unified">{html.escape(uni_col)}</td>'
            f'<td><span class="badge badge-map">MAP</span></td>'
            f'<td class="col-type">{html.escape(str(src_type))}</td>'
            f'<td class="col-type">{html.escape(str(uni_type))}</td>'
            f'<td class="{null_cls} col-null">{null_rate:.1%}</td>'
            f"</tr>"
        )

    # Use new classification if available, otherwise fall back to legacy gaps
    if derivable_gaps is not None:
        for gap in derivable_gaps:
            action = gap.get("action", "TYPE_CAST")
            badge_cls = "badge-derivable"
            src_col = gap.get("source_column") or "—"
            target_col = gap.get("target_column", "")
            src_type = gap.get("source_type") or "—"
            target_type = gap.get("target_type", "")

            rows.append(
                f"<tr>"
                f'<td class="col-source">{html.escape(str(src_col))}</td>'
                f"<td>&#8594;</td>"
                f'<td class="col-unified">{html.escape(target_col)}</td>'
                f'<td><span class="badge {badge_cls}">{html.escape(action)}</span></td>'
                f'<td class="col-type">{html.escape(str(src_type))}</td>'
                f'<td class="col-type">{html.escape(str(target_type))}</td>'
                f"<td>—</td>"
                f"</tr>"
            )
    if missing_columns is not None:
        # Build set of columns covered by enrichment or alias — don't show as MISSING
        covered_by_enrichment = set(enrichment_columns or [])
        covered_by_alias = {a.get("target", "") for a in (enrich_alias_ops or [])}
        skip_missing = covered_by_enrichment | covered_by_alias

        for mc in missing_columns:
            target_col = mc.get("target_column", "")
            if target_col in skip_missing:
                continue  # Will appear in enrichment or alias section instead
            target_type = mc.get("target_type", "")
            reason = mc.get("reason", "")

            rows.append(
                f"<tr>"
                f'<td class="col-source" style="color:#cf222e;">—</td>'
                f"<td>&#8594;</td>"
                f'<td class="col-unified">{html.escape(target_col)}</td>'
                f'<td><span class="badge badge-missing">MISSING</span></td>'
                f'<td class="col-type">—</td>'
                f'<td class="col-type">{html.escape(str(target_type))}</td>'
                f'<td title="{html.escape(reason)}" style="color:#cf222e;">unavailable</td>'
                f"</tr>"
            )
    elif not derivable_gaps:
        # Legacy fallback: use flat gaps list
        for gap in gaps:
            action = gap.get("action", "ADD")
            badge_cls = f"badge-{action.lower()}"
            src_col = gap.get("source_column") or "—"
            target_col = gap.get("target_column", "")
            src_type = gap.get("source_type") or "—"
            target_type = gap.get("target_type", "")

            rows.append(
                f"<tr>"
                f'<td class="col-source">{html.escape(str(src_col))}</td>'
                f"<td>&#8594;</td>"
                f'<td class="col-unified">{html.escape(target_col)}</td>'
                f'<td><span class="badge {badge_cls}">{html.escape(action)}</span></td>'
                f'<td class="col-type">{html.escape(str(src_type))}</td>'
                f'<td class="col-type">{html.escape(str(target_type))}</td>'
                f"<td>—</td>"
                f"</tr>"
            )

    # Enrichment columns (generated by pipeline blocks)
    if enrichment_columns:
        for col_name in enrichment_columns:
            uni_type = ""
            if unified_schema:
                uni_spec = unified_schema.get("columns", {}).get(col_name, {})
                uni_type = uni_spec.get("type", "")
            rows.append(
                f"<tr>"
                f'<td class="col-source" style="color:#1a7f37;">—</td>'
                f"<td>&#8594;</td>"
                f'<td class="col-unified">{html.escape(col_name)}</td>'
                f'<td><span class="badge badge-enrichment">ENRICHMENT</span></td>'
                f'<td class="col-type">—</td>'
                f'<td class="col-type">{html.escape(str(uni_type))}</td>'
                f'<td style="color:#1a7f37;">auto-generated</td>'
                f"</tr>"
            )

    # Enrich alias columns (required cols fulfilled by an enrichment col post-pipeline)
    if enrich_alias_ops:
        for alias in enrich_alias_ops:
            tgt = alias.get("target", "")
            src = alias.get("source", "")
            uni_type = ""
            if unified_schema:
                uni_spec = unified_schema.get("columns", {}).get(tgt, {})
                uni_type = uni_spec.get("type", "")
            rows.append(
                f"<tr>"
                f'<td class="col-source" style="color:#6e40c9;">—</td>'
                f"<td>&#8594;</td>"
                f'<td class="col-unified">{html.escape(tgt)}</td>'
                f'<td><span class="badge badge-alias">ALIAS</span></td>'
                f'<td class="col-type">—</td>'
                f'<td class="col-type">{html.escape(str(uni_type))}</td>'
                f'<td style="color:#6e40c9;" title="Filled from enrichment column \'{html.escape(src)}\' after pipeline runs">'
                f"&#8592; {html.escape(src)}</td>"
                f"</tr>"
            )

    return (
        '<table class="schema-table">'
        "<thead><tr>"
        "<th>Source Column</th><th></th><th>Unified Column</th>"
        "<th>Action</th><th>Source Type</th><th>Target Type</th><th>Null Rate</th>"
        "</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def render_missing_columns(missing_columns: list[dict]) -> str:
    """Render a table of missing columns with their reasons."""
    if not missing_columns:
        return ""

    rows = []
    for mc in missing_columns:
        target = mc.get("target_column", "?")
        target_type = mc.get("target_type", "?")
        reason = mc.get("reason", "No source data available")
        rows.append(
            f"<tr>"
            f'<td class="col-unified">{html.escape(target)}</td>'
            f'<td class="col-type">{html.escape(target_type)}</td>'
            f'<td><span class="badge badge-missing">UNAVAILABLE</span></td>'
            f'<td style="color:#57606a; font-size:0.85em;">{html.escape(reason)}</td>'
            f"</tr>"
        )

    return (
        '<table class="schema-table">'
        "<thead><tr><th>Column</th><th>Type</th><th>Status</th><th>Reason</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def render_yaml_review(operations: list[dict]) -> str:
    """Render YAML mapping operations for HITL review."""
    if not operations:
        return '<p style="color:#6e7781">No declarative operations.</p>'

    rows = []
    for op in operations:
        target = op.get("target", "?")
        col_type = op.get("type", "?")
        action = op.get("action", "?")
        source = op.get("source", "—")
        status = op.get("status", "")
        reason = op.get("reason", "")

        if action == "set_null":
            badge = '<span class="badge badge-missing">SET NULL</span>'
        elif action == "set_default":
            default_val = op.get("default_value", "?")
            badge = f'<span class="badge badge-map">DEFAULT: {html.escape(str(default_val))}</span>'
        elif action == "type_cast":
            badge = '<span class="badge badge-derivable">TYPE CAST</span>'
        elif action == "format_transform":
            transform = op.get("transform", "?")
            badge = f'<span class="badge badge-derivable">TRANSFORM: {html.escape(transform)}</span>'
        else:
            badge = f'<span class="badge">{html.escape(action)}</span>'

        detail = html.escape(reason) if reason else html.escape(str(source))

        rows.append(
            f"<tr>"
            f'<td class="col-unified">{html.escape(target)}</td>'
            f'<td class="col-type">{html.escape(col_type)}</td>'
            f"<td>{badge}</td>"
            f'<td style="color:#57606a; font-size:0.85em;">{detail}</td>'
            f"</tr>"
        )

    return (
        '<table class="schema-table">'
        "<thead><tr><th>Target Column</th><th>Type</th><th>Action</th><th>Detail</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def render_registry_results(hits: dict, misses: list[dict]) -> str:
    """Render registry hit/miss results."""
    rows = []
    for key in hits:
        rows.append(
            f'<tr><td class="col-unified">{html.escape(key)}</td>'
            f'<td><span class="badge badge-hit">HIT</span></td>'
            f'<td style="color:#3fb950">YAML mapping exists on disk — no LLM call needed</td></tr>'
        )
    for gap in misses:
        target = gap.get("target_column", "?")
        rows.append(
            f'<tr><td class="col-unified">{html.escape(target)}</td>'
            f'<td><span class="badge badge-miss">MISS</span></td>'
            f'<td style="color:#f0883e">Needs code generation via LLM</td></tr>'
        )
    return (
        '<table class="schema-table">'
        "<thead><tr><th>Gap / Key</th><th>Status</th><th>Action</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def render_code_review(func: dict) -> str:
    """Render a generated function for HITL code review."""
    fn_name = func.get("block_name", "?")
    code = html.escape(func.get("block_code", ""))
    passed = func.get("validation_passed", False)
    badge = (
        '<span class="badge badge-pass">PASSED</span>'
        if passed
        else '<span class="badge badge-fail">FAILED</span>'
    )
    sample_outputs = func.get("sample_outputs", {})

    # Sample I/O table
    io_rows = []
    for inp, out in sample_outputs.items():
        io_rows.append(
            f'<tr><td class="val-in">{html.escape(str(inp))}</td>'
            f'<td class="val-out">{html.escape(str(out))}</td></tr>'
        )
    io_table = ""
    if io_rows:
        io_table = (
            '<table class="io-table">'
            "<thead><tr><th>Input</th><th>Output</th></tr></thead>"
            f"<tbody>{''.join(io_rows)}</tbody>"
            "</table>"
        )

    return (
        f'<div class="code-review">'
        f'<div class="code-review-header">'
        f'<span class="fn-name">{html.escape(fn_name)}</span>'
        f"{badge}"
        f"</div>"
        f"<pre>{code}</pre>"
        f'<div class="validation-bar">Sample I/O</div>'
        f"{io_table}"
        f"</div>"
    )


def render_dq_cards(dq_pre: float, dq_post: float) -> str:
    """Render DQ score metric cards."""
    delta = dq_post - dq_pre
    delta_cls = "val-good" if delta > 0 else ("val-bad" if delta < 0 else "val-neutral")
    delta_sign = "+" if delta > 0 else ""

    return (
        f'<div class="metric-row">'
        f'<div class="metric-card">'
        f'<div class="metric-label">DQ Score (Pre)</div>'
        f'<div class="metric-value val-neutral">{dq_pre:.1f}%</div>'
        f'<div class="metric-sub">Before enrichment</div>'
        f"</div>"
        f'<div class="metric-card">'
        f'<div class="metric-label">DQ Score (Post)</div>'
        f'<div class="metric-value val-good">{dq_post:.1f}%</div>'
        f'<div class="metric-sub">After enrichment</div>'
        f"</div>"
        f'<div class="metric-card">'
        f'<div class="metric-label">DQ Delta</div>'
        f'<div class="metric-value {delta_cls}">{delta_sign}{delta:.1f}%</div>'
        f'<div class="metric-sub">Enrichment contribution</div>'
        f"</div>"
        f"</div>"
    )


def render_summary_cards(
    rows: int, clusters: int, registry_hits: int, functions_generated: int
) -> str:
    """Render summary metric cards."""
    return (
        f'<div class="metric-row">'
        f'<div class="metric-card">'
        f'<div class="metric-label">Output Rows</div>'
        f'<div class="metric-value val-neutral">{rows}</div>'
        f"</div>"
        f'<div class="metric-card">'
        f'<div class="metric-label">Registry Hits</div>'
        f'<div class="metric-value val-good">{registry_hits}</div>'
        f'<div class="metric-sub">Reused transforms</div>'
        f"</div>"
        f'<div class="metric-card">'
        f'<div class="metric-label">Generated</div>'
        f'<div class="metric-value val-warn">{functions_generated}</div>'
        f'<div class="metric-sub">New transforms</div>'
        f"</div>"
        f"</div>"
    )


def render_block_waterfall(audit_log: list[dict]) -> str:
    """Render block execution as a horizontal waterfall chart."""
    if not audit_log:
        return '<p style="color:#6e7781">No audit log entries.</p>'

    max_rows = max((e.get("rows_in", 0) for e in audit_log), default=1) or 1

    bars = []
    for entry in audit_log:
        name = entry.get("block", "?")
        r_in = entry.get("rows_in", 0)
        r_out = entry.get("rows_out", 0)
        pct = (r_out / max_rows * 100) if max_rows > 0 else 0
        bar_cls = "loss" if r_out < r_in else ""

        bars.append(
            f'<div class="waterfall-row">'
            f'<div class="waterfall-label">{html.escape(name)}</div>'
            f'<div class="waterfall-bar-wrap">'
            f'<div class="waterfall-bar {bar_cls}" style="width:{pct:.0f}%"></div>'
            f'<div class="waterfall-count">{r_in} &#8594; {r_out}</div>'
            f"</div>"
            f"</div>"
        )

    return f'<div class="waterfall">{"".join(bars)}</div>'


def render_enrichment_breakdown(stats: dict) -> str:
    """Render enrichment tier breakdown as horizontal bars."""
    if not stats:
        return '<p style="color:#6e7781">No enrichment stats available.</p>'

    total = sum(stats.values()) or 1
    tiers = [
        ("Deterministic", stats.get("deterministic", 0), "tier-1"),
        ("Embedding", stats.get("embedding", 0), "tier-2"),
        ("Propagation", stats.get("propagation", 0), "tier-3"),
        ("LLM", stats.get("llm", 0), "tier-4"),
    ]

    bars = []
    for label, count, cls in tiers:
        pct = (count / total * 100) if total > 0 else 0
        bars.append(
            f'<div class="enrich-row">'
            f'<div class="enrich-tier">{label}</div>'
            f'<div class="enrich-bar-wrap">'
            f'<div class="enrich-bar {cls}" style="width:{max(pct, 2):.0f}%"></div>'
            f'<div class="enrich-count">{count} ({pct:.0f}%)</div>'
            f"</div>"
            f"</div>"
        )

    unresolved = stats.get("unresolved", 0)
    if unresolved > 0:
        bars.append(
            f'<div class="enrich-row">'
            f'<div class="enrich-tier" style="color:#f85149">Unresolved</div>'
            f'<div class="enrich-bar-wrap">'
            f'<div class="enrich-count" style="color:#f85149">{unresolved} rows</div>'
            f"</div>"
            f"</div>"
        )

    return f'<div class="enrich-breakdown">{"".join(bars)}</div>'


def render_run_history(runs: list[dict]) -> str:
    """Render a comparison table of all completed pipeline runs."""
    rows = []
    for r in runs:
        delta = r.get("dq_delta", 0)
        delta_cls = (
            "val-good" if delta > 0 else ("val-bad" if delta < 0 else "val-neutral")
        )
        delta_sign = "+" if delta > 0 else ""
        schema_badge = (
            '<span class="badge badge-hit">EXISTS</span>'
            if r.get("schema_existed")
            else '<span class="badge badge-miss">DERIVED</span>'
        )
        rows.append(
            f"<tr>"
            f"<td>Run {r['run_num']}</td>"
            f"<td>{html.escape(r['source'])}</td>"
            f"<td>{html.escape(r['domain'])}</td>"
            f"<td>{r['rows']}</td>"
            f"<td>{r['dq_pre']:.1f}% → {r['dq_post']:.1f}%</td>"
            f'<td class="{delta_cls}">{delta_sign}{delta:.1f}%</td>'
            f"<td>{r['registry_hits']}</td>"
            f"<td>{r['functions_generated']}</td>"
            f"<td>{schema_badge}</td>"
            f"</tr>"
        )
    return (
        '<table class="schema-table">'
        "<thead><tr>"
        "<th>#</th><th>Source</th><th>Domain</th><th>Rows</th>"
        "<th>DQ (Pre→Post)</th><th>Delta</th><th>Reg. Hits</th><th>Generated</th><th>Schema</th>"
        "</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def render_quarantine_table(quarantine_reasons: list[dict], df=None) -> str:
    """Render quarantined rows table."""
    if not quarantine_reasons:
        return (
            '<div style="background:#dafbe1; border:1px solid #4ac26b; border-radius:8px; '
            'padding:16px; color:#1a7f37; text-align:center; margin:0.5rem 0;">'
            "&#10003; All rows passed post-enrichment validation"
            "</div>"
        )

    rows = []
    for item in quarantine_reasons[:50]:  # Cap at 50 for display
        idx = item.get("row_idx", "?")
        missing = ", ".join(item.get("missing_fields", []))
        reason = item.get("reason", "")
        product = ""
        if df is not None and "product_name" in df.columns:
            try:
                product = str(df.at[idx, "product_name"])[:60]
            except Exception:
                pass

        rows.append(
            f"<tr>"
            f"<td>{idx}</td>"
            f"<td>{html.escape(product)}</td>"
            f'<td class="reason">{html.escape(missing)}</td>'
            f"</tr>"
        )

    return (
        f'<table class="quarantine-table">'
        f"<thead><tr><th>Row</th><th>Product</th><th>Missing Fields</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        f"</table>"
        f'<p style="color:#f85149; font-size:0.82rem; margin-top:8px;">'
        f"{len(quarantine_reasons)} rows quarantined</p>"
    )


def render_agent_header(agent_num: int, role: str, activity: str) -> str:
    """
    Render an Agent header with distinct styling.

    Args:
        agent_num: Agent number (1, 2, or 3)
        role: Role name (Orchestrator, Critic, Sequence Planner)
        activity: Current activity description

    Returns:
        HTML string with agent header
    """
    colors = {
        1: ("#0969da", "Agent 1"),
        2: ("#8250df", "Agent 2"),
        3: ("#1a7f37", "Agent 3"),
    }
    color, label = colors.get(agent_num, ("#57606a", f"Agent {agent_num}"))

    return (
        f'<div class="agent-header" style="'
        f"background: linear-gradient(135deg, {color}15, {color}08); "
        f"border-left: 4px solid {color}; "
        f"padding: 12px 16px; "
        f"margin: 16px 0; "
        f'border-radius: 0 8px 8px 0;">'
        f'<span style="font-weight: 600; color: {color};">{label}</span> '
        f'<span style="color: #24292f; font-weight: 500;">({role})</span>'
        f'<div style="color: #57606a; font-size: 0.9em; margin-top: 4px;">{html.escape(activity)}</div>'
        f"</div>"
    )


def render_sampling_stats(strategy: dict) -> str:
    """
    Render Agent 1's sampling strategy statistics.

    Args:
        strategy: SamplingStrategy dataclass as dict with method, sample_size,
                  fallback_triggered, fallback_reason

    Returns:
        HTML string with sampling stats
    """
    method = strategy.get("method", "unknown")
    sample_size = strategy.get("sample_size", 0)
    fallback = strategy.get("fallback_triggered", False)
    reason = strategy.get("fallback_reason", "")

    fallback_html = ""
    if fallback:
        fallback_html = f'<span class="badge badge-warn">Fallback: {html.escape(str(reason))}</span>'

    return (
        f'<div class="sampling-stats" style="'
        f'background: #f6f8fa; padding: 12px; border-radius: 6px; margin: 12px 0;">'
        f'<div style="font-size: 0.85em; color: #57606a; margin-bottom: 8px;">'
        f"<strong>Agent 1 Sampling Strategy</strong></div>"
        f'<div style="display: flex; gap: 16px; flex-wrap: wrap;">'
        f'<div><span style="color: #24292f; font-weight: 500;">Method:</span> '
        f"<code>{html.escape(method)}</code></div>"
        f'<div><span style="color: #24292f; font-weight: 500;">Sample Size:</span> '
        f"<span>{sample_size:,}</span></div>"
        f"{fallback_html}"
        f"</div>"
        f"</div>"
    )


def render_confidence_badge(score: float) -> str:
    """
    Render a confidence score badge.

    Args:
        score: Confidence score between 0.0 and 1.0

    Returns:
        HTML string with badge
    """
    if score >= 0.9:
        return '<span class="badge badge-pass">High</span>'
    elif score >= 0.5:
        return '<span class="badge badge-warn">Medium</span>'
    else:
        return '<span class="badge badge-fail">Low</span>'


def render_extraction_only_flag() -> str:
    """
    Render the extraction-only badge for safety constraint columns.

    Returns:
        HTML string with extraction-only badge
    """
    return '<span class="badge badge-enrichment">EXTRACTION-ONLY</span>'


def render_log_panel(
    entries: list[dict],
    level_filter: str = "ALL",
    step_filter: str = "ALL",
    tall: bool = False,
    max_entries: int = 500,
) -> str:
    """Render structured log entries as a dark terminal-style panel."""
    filtered = entries
    if level_filter != "ALL":
        filtered = [e for e in filtered if e.get("level") == level_filter]
    if step_filter != "ALL":
        filtered = [e for e in filtered if str(e.get("step", "")) == step_filter]
    if len(filtered) > max_entries:
        filtered = filtered[-max_entries:]

    lines = []
    for e in filtered:
        t = html.escape(str(e.get("time", "")))
        level = str(e.get("level", "INFO"))
        logger_name = html.escape(str(e.get("logger", "")))
        event = html.escape(str(e.get("event", "")))
        lcls = _LOG_LEVEL_CSS.get(level, "log-info")
        lines.append(
            f'<div class="log-entry">'
            f'<span class="log-time">{t}</span> '
            f'<span class="{lcls}">[{html.escape(level)}]</span> '
            f'<span class="log-logger">{logger_name}</span>'
            f'<span class="log-text">: {event}</span>'
            f"</div>"
        )

    panel_cls = "log-panel log-panel-tall" if tall else "log-panel"
    content = (
        "".join(lines)
        if lines
        else '<div class="log-entry log-debug">— no entries —</div>'
    )
    return f'<div class="{panel_cls}">{content}</div>'


def render_operations_review(operations: list[dict]) -> str:
    """Render LLM-format revised_operations for step 2 HITL review.

    Handles the operations format returned by Agent 1/2, which uses
    primitive, source_column, target_column keys (not the YAML format).
    """
    if not operations:
        return '<p style="color:#6e7781">No operations to review.</p>'

    rows = []
    for op in operations:
        primitive = str(op.get("primitive", "?"))
        target = html.escape(str(op.get("target_column", op.get("target", "?"))))
        source = html.escape(str(op.get("source_column", op.get("source", "—"))))
        action = str(op.get("action", "?"))
        target_type = html.escape(str(op.get("target_type", "?")))

        prim_cls = _PRIMITIVE_BADGE_CLS.get(primitive, "badge-map")
        prim_html = f'<span class="badge {prim_cls}">{html.escape(primitive)}</span>'
        act_html = _ACTION_BADGE.get(action, f'<span class="badge">{html.escape(action)}</span>')

        rows.append(
            f"<tr>"
            f"<td>{prim_html}</td>"
            f'<td class="col-source">{source}</td>'
            f'<td style="color:#6e7781">&#8594;</td>'
            f'<td class="col-unified">{target}</td>'
            f'<td class="col-type">{target_type}</td>'
            f"<td>{act_html}</td>"
            f"</tr>"
        )

    return (
        '<table class="schema-table">'
        "<thead><tr>"
        "<th>Primitive</th><th>Source</th><th></th><th>Target</th><th>Type</th><th>Action</th>"
        "</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def render_critique_notes(notes: list[dict]) -> str:
    """Render Agent 2 correction notes table."""
    if not notes:
        return (
            '<div style="background:#dafbe1; border:1px solid #4ac26b; border-radius:6px; '
            'padding:10px 14px; color:#1a7f37; font-size:0.85em;">'
            "&#10003; No corrections — Agent 1 output accepted as-is."
            "</div>"
        )
    rows = []
    for note in notes:
        rule = html.escape(str(note.get("rule", "?")))
        col = html.escape(str(note.get("column", "?")))
        correction = html.escape(str(note.get("correction", ""))[:150])
        rows.append(
            f"<tr>"
            f'<td><span class="badge badge-derivable">{rule}</span></td>'
            f'<td class="col-unified">{col}</td>'
            f'<td style="color:#57606a; font-size:0.85em;">{correction}</td>'
            f"</tr>"
        )
    return (
        '<table class="schema-table">'
        "<thead><tr><th>Rule</th><th>Column</th><th>Correction</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def render_block_metrics_table(audit_log: list[dict]) -> str:
    """Render per-block row counts with pass/fail status."""
    if not audit_log:
        return '<p style="color:#6e7781">No audit log entries.</p>'
    rows = []
    for entry in audit_log:
        block = html.escape(str(entry.get("block", "?")))
        r_in = entry.get("rows_in", 0)
        r_out = entry.get("rows_out", 0)
        delta = r_out - r_in
        is_loss = r_out < r_in * 0.5 and r_in > 0
        status_badge = (
            '<span class="badge badge-fail">&#9888; LOSS</span>'
            if is_loss
            else '<span class="badge badge-pass">&#10003;</span>'
        )
        delta_str = f"{delta:+,}" if delta != 0 else "0"
        delta_cls = "val-bad" if delta < 0 else ("val-neutral" if delta == 0 else "val-good")
        rows.append(
            f"<tr>"
            f'<td class="col-unified">{block}</td>'
            f"<td>{r_in:,}</td>"
            f"<td>{r_out:,}</td>"
            f'<td class="{delta_cls}" style="font-family:monospace">{delta_str}</td>'
            f"<td>{status_badge}</td>"
            f"</tr>"
        )
    return (
        '<table class="schema-table">'
        "<thead><tr><th>Block</th><th>Rows In</th><th>Rows Out</th><th>Delta</th><th>Status</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def render_hitl_gate(gate_num: int, gate_type: str, options: list[str]) -> str:
    """
    Render a HITL approval gate.

    Args:
        gate_num: Gate number (1 or 3)
        gate_type: Type of gate (Schema Mapping, Quarantine)
        options: List of button label options

    Returns:
        HTML string with HITL gate UI
    """
    return (
        f'<div class="hitl-gate" style="'
        f"border: 2px dashed #f0883e; "
        f"background: #fff8c5; "
        f"padding: 16px; "
        f"border-radius: 8px; "
        f'margin: 16px 0;">'
        f'<div style="font-weight: 600; color: #9a6700; margin-bottom: 8px;">'
        f"HITL Gate {gate_num}: {html.escape(gate_type)}</div>"
        f'<div style="font-size: 0.85em; color: #57606a;">'
        f"Review the above and select an action to proceed.</div>"
        f"</div>"
    )
