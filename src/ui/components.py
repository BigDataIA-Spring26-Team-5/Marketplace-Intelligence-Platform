"""Custom HTML components for the ETL pipeline Streamlit UI."""

from __future__ import annotations

import html


def render_step_bar(current_step: int, steps: list[str], max_completed: int = -1) -> str:
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
            f'</div>'
        )
    return f'<div class="step-bar">{"".join(items)}</div>'


def render_source_profile(profile: dict) -> str:
    """Render a source schema profile as an HTML table."""
    rows = []
    for col, info in profile.items():
        null_rate = info.get("null_rate", 0)
        null_cls = "null-low" if null_rate < 0.1 else ("null-mid" if null_rate < 0.4 else "null-high")
        samples = ", ".join(str(s)[:50] for s in info.get("sample_values", [])[:3])
        rows.append(
            f'<tr>'
            f'<td class="col-name">{html.escape(col)}</td>'
            f'<td class="col-type">{html.escape(str(info.get("dtype", "")))}</td>'
            f'<td class="{null_cls} col-null">{null_rate:.1%}</td>'
            f'<td>{info.get("unique_count", "")}</td>'
            f'<td class="sample">{html.escape(samples)}</td>'
            f'</tr>'
        )
    return (
        '<table class="profile-table">'
        '<thead><tr><th>Column</th><th>Type</th><th>Null Rate</th><th>Unique</th><th>Samples</th></tr></thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        '</table>'
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
        null_cls = "null-low" if null_rate < 0.1 else ("null-mid" if null_rate < 0.4 else "null-high")
        src_type = src_info.get("dtype", "?")
        uni_type = ""
        if unified_schema:
            uni_spec = unified_schema.get("columns", {}).get(uni_col, {})
            uni_type = uni_spec.get("type", "")

        rows.append(
            f'<tr>'
            f'<td class="col-source">{html.escape(src_col)}</td>'
            f'<td>&#8594;</td>'
            f'<td class="col-unified">{html.escape(uni_col)}</td>'
            f'<td><span class="badge badge-map">MAP</span></td>'
            f'<td class="col-type">{html.escape(str(src_type))}</td>'
            f'<td class="col-type">{html.escape(str(uni_type))}</td>'
            f'<td class="{null_cls} col-null">{null_rate:.1%}</td>'
            f'</tr>'
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
                f'<tr>'
                f'<td class="col-source">{html.escape(str(src_col))}</td>'
                f'<td>&#8594;</td>'
                f'<td class="col-unified">{html.escape(target_col)}</td>'
                f'<td><span class="badge {badge_cls}">{html.escape(action)}</span></td>'
                f'<td class="col-type">{html.escape(str(src_type))}</td>'
                f'<td class="col-type">{html.escape(str(target_type))}</td>'
                f'<td>—</td>'
                f'</tr>'
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
                f'<tr>'
                f'<td class="col-source" style="color:#cf222e;">—</td>'
                f'<td>&#8594;</td>'
                f'<td class="col-unified">{html.escape(target_col)}</td>'
                f'<td><span class="badge badge-missing">MISSING</span></td>'
                f'<td class="col-type">—</td>'
                f'<td class="col-type">{html.escape(str(target_type))}</td>'
                f'<td title="{html.escape(reason)}" style="color:#cf222e;">unavailable</td>'
                f'</tr>'
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
                f'<tr>'
                f'<td class="col-source">{html.escape(str(src_col))}</td>'
                f'<td>&#8594;</td>'
                f'<td class="col-unified">{html.escape(target_col)}</td>'
                f'<td><span class="badge {badge_cls}">{html.escape(action)}</span></td>'
                f'<td class="col-type">{html.escape(str(src_type))}</td>'
                f'<td class="col-type">{html.escape(str(target_type))}</td>'
                f'<td>—</td>'
                f'</tr>'
            )

    # Enrichment columns (generated by pipeline blocks)
    if enrichment_columns:
        for col_name in enrichment_columns:
            uni_type = ""
            if unified_schema:
                uni_spec = unified_schema.get("columns", {}).get(col_name, {})
                uni_type = uni_spec.get("type", "")
            rows.append(
                f'<tr>'
                f'<td class="col-source" style="color:#1a7f37;">—</td>'
                f'<td>&#8594;</td>'
                f'<td class="col-unified">{html.escape(col_name)}</td>'
                f'<td><span class="badge badge-enrichment">ENRICHMENT</span></td>'
                f'<td class="col-type">—</td>'
                f'<td class="col-type">{html.escape(str(uni_type))}</td>'
                f'<td style="color:#1a7f37;">auto-generated</td>'
                f'</tr>'
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
                f'<tr>'
                f'<td class="col-source" style="color:#6e40c9;">—</td>'
                f'<td>&#8594;</td>'
                f'<td class="col-unified">{html.escape(tgt)}</td>'
                f'<td><span class="badge badge-alias">ALIAS</span></td>'
                f'<td class="col-type">—</td>'
                f'<td class="col-type">{html.escape(str(uni_type))}</td>'
                f'<td style="color:#6e40c9;" title="Filled from enrichment column \'{html.escape(src)}\' after pipeline runs">'
                f'&#8592; {html.escape(src)}</td>'
                f'</tr>'
            )

    return (
        '<table class="schema-table">'
        '<thead><tr>'
        '<th>Source Column</th><th></th><th>Unified Column</th>'
        '<th>Action</th><th>Source Type</th><th>Target Type</th><th>Null Rate</th>'
        '</tr></thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        '</table>'
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
            f'<tr>'
            f'<td class="col-unified">{html.escape(target)}</td>'
            f'<td class="col-type">{html.escape(target_type)}</td>'
            f'<td><span class="badge badge-missing">UNAVAILABLE</span></td>'
            f'<td style="color:#57606a; font-size:0.85em;">{html.escape(reason)}</td>'
            f'</tr>'
        )

    return (
        '<table class="schema-table">'
        '<thead><tr><th>Column</th><th>Type</th><th>Status</th><th>Reason</th></tr></thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        '</table>'
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
            f'<tr>'
            f'<td class="col-unified">{html.escape(target)}</td>'
            f'<td class="col-type">{html.escape(col_type)}</td>'
            f'<td>{badge}</td>'
            f'<td style="color:#57606a; font-size:0.85em;">{detail}</td>'
            f'</tr>'
        )

    return (
        '<table class="schema-table">'
        '<thead><tr><th>Target Column</th><th>Type</th><th>Action</th><th>Detail</th></tr></thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        '</table>'
    )


def render_registry_results(hits: dict, misses: list[dict]) -> str:
    """Render registry hit/miss results."""
    rows = []
    for key in hits:
        rows.append(
            f'<tr><td class="col-unified">{html.escape(key)}</td>'
            f'<td><span class="badge badge-hit">HIT</span></td>'
            f'<td style="color:#3fb950">Reusing saved function (zero cost)</td></tr>'
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
        '<thead><tr><th>Gap / Key</th><th>Status</th><th>Action</th></tr></thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        '</table>'
    )


def render_code_review(func: dict) -> str:
    """Render a generated function for HITL code review."""
    fn_name = func.get("block_name", "?")
    code = html.escape(func.get("block_code", ""))
    passed = func.get("validation_passed", False)
    badge = '<span class="badge badge-pass">PASSED</span>' if passed else '<span class="badge badge-fail">FAILED</span>'
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
            '<thead><tr><th>Input</th><th>Output</th></tr></thead>'
            f'<tbody>{"".join(io_rows)}</tbody>'
            '</table>'
        )

    return (
        f'<div class="code-review">'
        f'<div class="code-review-header">'
        f'<span class="fn-name">{html.escape(fn_name)}</span>'
        f'{badge}'
        f'</div>'
        f'<pre>{code}</pre>'
        f'<div class="validation-bar">Sample I/O</div>'
        f'{io_table}'
        f'</div>'
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
        f'</div>'
        f'<div class="metric-card">'
        f'<div class="metric-label">DQ Score (Post)</div>'
        f'<div class="metric-value val-good">{dq_post:.1f}%</div>'
        f'<div class="metric-sub">After enrichment</div>'
        f'</div>'
        f'<div class="metric-card">'
        f'<div class="metric-label">DQ Delta</div>'
        f'<div class="metric-value {delta_cls}">{delta_sign}{delta:.1f}%</div>'
        f'<div class="metric-sub">Enrichment contribution</div>'
        f'</div>'
        f'</div>'
    )


def render_summary_cards(rows: int, clusters: int, registry_hits: int, functions_generated: int) -> str:
    """Render summary metric cards."""
    return (
        f'<div class="metric-row">'
        f'<div class="metric-card">'
        f'<div class="metric-label">Output Rows</div>'
        f'<div class="metric-value val-neutral">{rows}</div>'
        f'</div>'
        f'<div class="metric-card">'
        f'<div class="metric-label">Registry Hits</div>'
        f'<div class="metric-value val-good">{registry_hits}</div>'
        f'<div class="metric-sub">Reused transforms</div>'
        f'</div>'
        f'<div class="metric-card">'
        f'<div class="metric-label">Generated</div>'
        f'<div class="metric-value val-warn">{functions_generated}</div>'
        f'<div class="metric-sub">New transforms</div>'
        f'</div>'
        f'</div>'
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
            f'</div>'
            f'</div>'
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
            f'</div>'
            f'</div>'
        )

    unresolved = stats.get("unresolved", 0)
    if unresolved > 0:
        bars.append(
            f'<div class="enrich-row">'
            f'<div class="enrich-tier" style="color:#f85149">Unresolved</div>'
            f'<div class="enrich-bar-wrap">'
            f'<div class="enrich-count" style="color:#f85149">{unresolved} rows</div>'
            f'</div>'
            f'</div>'
        )

    return f'<div class="enrich-breakdown">{"".join(bars)}</div>'


def render_pipeline_remembered(hits: dict) -> str:
    """Render a 'Pipeline Remembered' banner when all gaps are registry hits."""
    rows = "".join(f'<li><code>{html.escape(k)}</code></li>' for k in hits)
    count = len(hits)
    return (
        '<div class="remembered-banner">'
        f'<div class="remembered-title">&#9679; Pipeline Remembered ({count} function{"s" if count != 1 else ""})</div>'
        '<ul class="remembered-list">'
        f'{rows}'
        '</ul>'
        '<div class="remembered-sub">All schema gaps covered by the function registry — Agent 2 was not called.</div>'
        '</div>'
    )


def render_run_history(runs: list[dict]) -> str:
    """Render a comparison table of all completed pipeline runs."""
    rows = []
    for r in runs:
        delta = r.get("dq_delta", 0)
        delta_cls = "val-good" if delta > 0 else ("val-bad" if delta < 0 else "val-neutral")
        delta_sign = "+" if delta > 0 else ""
        schema_badge = (
            '<span class="badge badge-hit">EXISTS</span>'
            if r.get("schema_existed")
            else '<span class="badge badge-miss">DERIVED</span>'
        )
        rows.append(
            f'<tr>'
            f'<td>Run {r["run_num"]}</td>'
            f'<td>{html.escape(r["source"])}</td>'
            f'<td>{html.escape(r["domain"])}</td>'
            f'<td>{r["rows"]}</td>'
            f'<td>{r["dq_pre"]:.1f}% → {r["dq_post"]:.1f}%</td>'
            f'<td class="{delta_cls}">{delta_sign}{delta:.1f}%</td>'
            f'<td>{r["registry_hits"]}</td>'
            f'<td>{r["functions_generated"]}</td>'
            f'<td>{schema_badge}</td>'
            f'</tr>'
        )
    return (
        '<table class="schema-table">'
        '<thead><tr>'
        '<th>#</th><th>Source</th><th>Domain</th><th>Rows</th>'
        '<th>DQ (Pre→Post)</th><th>Delta</th><th>Reg. Hits</th><th>Generated</th><th>Schema</th>'
        '</tr></thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        '</table>'
    )


def render_quarantine_table(quarantine_reasons: list[dict], df=None) -> str:
    """Render quarantined rows table."""
    if not quarantine_reasons:
        return (
            '<div style="background:#dafbe1; border:1px solid #4ac26b; border-radius:8px; '
            'padding:16px; color:#1a7f37; text-align:center; margin:0.5rem 0;">'
            '&#10003; All rows passed post-enrichment validation'
            '</div>'
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
            f'<tr>'
            f'<td>{idx}</td>'
            f'<td>{html.escape(product)}</td>'
            f'<td class="reason">{html.escape(missing)}</td>'
            f'</tr>'
        )

    return (
        f'<table class="quarantine-table">'
        f'<thead><tr><th>Row</th><th>Product</th><th>Missing Fields</th></tr></thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        f'</table>'
        f'<p style="color:#f85149; font-size:0.82rem; margin-top:8px;">'
        f'{len(quarantine_reasons)} rows quarantined</p>'
    )
