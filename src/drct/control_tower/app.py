"""Data Reliability Control Tower: six reliability panels over the monitoring schema.

Reads through ``CONTROL_TOWER_DSN`` (role ``control_tower_ro``) via ``queries.connect`` and
re-queries at most every 15 seconds.
"""

import os
from datetime import datetime
from html import escape

import pandas as pd
import plotly.graph_objects as go
import psycopg
import streamlit as st
from plotly.subplots import make_subplots

from drct.control_tower import queries, theme
from drct.control_tower.queries import HealthSummary, readable_node

st.set_page_config(
    page_title="Data Reliability Control Tower",
    page_icon=":material/cell_tower:",
    layout="wide",
)
st.markdown(theme.CSS, unsafe_allow_html=True)

EMPTY = '<div class="ct-empty">No data yet — run <code>make seed</code></div>'


@st.cache_data(ttl=15, show_spinner=False)
def load(dsn: str) -> dict:
    with queries.connect(dsn) as conn:
        return {
            "health": queries.health_summary(conn),
            "freshness": queries.freshness_status(conn),
            "quality": queries.quality_trend(conn),
            "volume": queries.volume_series(conn),
            "failed": queries.failed_tests(conn),
            "sla": queries.sla_timeline(conn),
            "incidents": queries.incidents(conn),
        }


# ---------------------------------------------------------------- formatting helpers


def sim_label(ts: datetime | None) -> str:
    return "—" if ts is None or pd.isna(ts) else f"{ts:%a %d %b %Y · %H:%M} UTC"


def short_date(ts) -> str:
    return "—" if ts is None or pd.isna(ts) else f"{ts:%d %b}"


def state_var(status: str) -> str:
    return f"--state:{theme.status_style(status)[0]}"


def chip(status: str, text: str) -> str:
    _, glyph, _ = theme.status_style(status)
    return f'<span class="ct-chip" style="{state_var(status)}"><b>{glyph}</b>{escape(text)}</span>'


def panel_head(title: str, caption: str, status_chip: str = "") -> None:
    st.markdown(
        f'<div class="ct-panel-head"><span class="ct-panel-title">{escape(title)}</span>'
        f'{status_chip}</div><div class="ct-caption">{escape(caption)}</div>',
        unsafe_allow_html=True,
    )


def note(text: str, status: str = "healthy") -> None:
    _, glyph, _ = theme.status_style(status)
    st.markdown(
        f'<div class="ct-empty" style="{state_var(status)}">'
        f'<span style="color:var(--state)">{glyph}</span>&nbsp; {escape(text)}</div>',
        unsafe_allow_html=True,
    )


def sla_now(sla: pd.DataFrame) -> pd.DataFrame:
    """Each rule's newest day, whose ``latest_status`` is where the rule stands now."""
    return sla.sort_values("sim_date").groupby("rule").tail(1)


def plot(fig: go.Figure) -> None:
    st.plotly_chart(fig, width="stretch", theme=None, config=theme.PLOTLY_CONFIG)


# ---------------------------------------------------------------- header and KPI strip


def header(health: HealthSummary) -> None:
    clock = (
        sim_label(health.sim_now)
        if health.sim_now is not None
        else "clock not started — run make seed"
    )
    st.markdown(
        '<div class="ct-top"><div class="ct-brand">Data Reliability Control Tower'
        "<small>Freshness, quality, volume and SLAs for the sales &amp; payments pipeline — "
        "watched from the raw files to the dashboard.</small></div>"
        f'<div class="ct-clock"><span>Simulated date</span>{escape(clock)}</div></div>',
        unsafe_allow_html=True,
    )


def tile(label: str, value: str, sub: str, status: str | None = None, hero: bool = False) -> str:
    style = f' style="{state_var(status)}"' if status else ""
    glyph = f'<span class="glyph">{theme.status_style(status)[1]}</span>' if status else ""
    return (
        f'<div class="ct-tile{" hero" if hero else ""}"{style}><div class="lbl">{escape(label)}</div>'
        f'<div class="val">{glyph}{escape(value)}</div><div class="sub">{escape(sub)}</div></div>'
    )


def kpi_strip(data: dict) -> None:
    health: HealthSummary = data["health"]
    fresh, quality, sla = data["freshness"], data["quality"], data["sla"]
    _, _, label = theme.status_style(health.status)
    open_total = health.open_critical + health.open_warning
    if health.status == "no_data":
        hero_sub = "Seed the warehouse to start monitoring"
    elif open_total:
        hero_sub = f"{open_total} open incident{'s' * (open_total != 1)}"
    else:
        hero_sub = "No open incidents"
    tiles = [
        tile("Overall health", label, hero_sub, health.status, hero=True),
        tile("Open critical", str(health.open_critical), "incidents", None),
        tile("Open warnings", str(health.open_warning), "incidents", None),
    ]
    if fresh.empty:
        tiles.append(tile("Sources fresh", "—", "no freshness checks yet"))
    else:
        stale = fresh.loc[fresh["status"] != "pass", "source"].tolist()
        tiles.append(
            tile(
                "Sources fresh",
                f"{len(fresh) - len(stale)}/{len(fresh)}",
                f"stale: {', '.join(stale)}" if stale else "all within 20h",
            )
        )
    latest_rate = quality["pass_rate"].iloc[-1] if not quality.empty else None
    if latest_rate is None or pd.isna(latest_rate):
        tiles.append(tile("Tests passing", "—", "no dbt runs yet"))
    else:
        failing = int(quality["failed"].iloc[-1]) + int(quality["warned"].iloc[-1])
        tiles.append(
            tile(
                "Tests passing",
                f"{latest_rate:.0%}",
                f"latest dbt run · {failing} not passing" if failing else "latest dbt run",
            )
        )
    if sla.empty:
        tiles.append(tile("SLAs met", "—", "no SLA checks yet"))
    else:
        now = sla_now(sla)
        met = int((now["latest_status"] == "met").sum())
        breached = now.loc[now["latest_status"] == "breached", "rule"].tolist()
        tiles.append(
            tile(
                "SLAs met",
                f"{met}/{len(now)}",
                f"breached: {', '.join(breached)}"
                if breached
                else f"{len(now) - met} pending"
                if met < len(now)
                else "latest check per rule",
            )
        )
    st.markdown(f'<div class="ct-strip">{"".join(tiles)}</div>', unsafe_allow_html=True)


# ---------------------------------------------------------------- panels


def freshness_panel(fresh: pd.DataFrame) -> None:
    stale = int((fresh["status"] != "pass").sum()) if not fresh.empty else 0
    worst = (
        "error"
        if fresh["status"].isin(["error", "runtime error"]).any()
        else "warn"
        if stale
        else "pass"
    )
    head_chip = "" if fresh.empty else chip(worst, f"{len(fresh) - stale}/{len(fresh)} fresh")
    panel_head(
        "Freshness",
        "Hours since each raw source last landed, against the 20h warn and 44h error limits.",
        head_chip,
    )
    if fresh.empty:
        st.markdown(EMPTY, unsafe_allow_html=True)
        return
    frame = fresh.sort_values("source", ascending=False)
    styles = [theme.status_style(s) for s in frame["status"]]
    ages = frame["age_hours"].fillna(0)
    labels = [
        f"{a:.1f}h · {lbl}" if pd.notna(a) else lbl
        for a, (_, _, lbl) in zip(frame["age_hours"], styles, strict=True)
    ]
    fig = go.Figure(
        go.Bar(
            x=ages,
            y=frame["source"],
            orientation="h",
            marker={"color": [c for c, _, _ in styles]},
            text=labels,
            textposition="outside",
            textfont={"color": theme.INK_2, "family": theme.MONO, "size": 11},
            cliponaxis=False,
            hovertemplate="%{y}: %{x:.1f}h since last load<extra></extra>",
        )
    )
    error_h = int(frame["error_hours"].iloc[0])
    warn_h = int(frame["warn_hours"].iloc[0])
    top = max(error_h * 1.25, float(ages.max()) * 1.3)
    for hours, colour, text in (
        (warn_h, theme.WARNING, "warn"),
        (error_h, theme.CRITICAL, "error"),
    ):
        fig.add_vline(x=hours, line={"color": colour, "width": 1, "dash": "dot"})
        fig.add_annotation(
            x=hours,
            y=1,
            yref="paper",
            yanchor="bottom",
            text=f"{text} {hours}h",
            showarrow=False,
            font={"color": theme.MUTED, "size": 10, "family": theme.MONO},
        )
    layout = theme.chart_layout(height=40 * len(frame) + 40)
    layout["margin"]["t"] = 22
    layout["xaxis"].update(range=[0, top], ticksuffix="h")
    layout["yaxis"].update(
        showgrid=False, tickfont={"color": theme.INK, "size": 12, "family": theme.MONO}
    )
    fig.update_layout(**layout, bargap=0.45, barcornerradius=4)
    plot(fig)
    st.markdown(
        f'<div class="ct-legend">age since last load<i style="{state_var("pass")}">●</i>fresh'
        f'<i style="{state_var("warn")}">▲</i>past 20h<i style="{state_var("error")}">◆</i>past 44h</div>',
        unsafe_allow_html=True,
    )


def quality_panel(quality: pd.DataFrame) -> None:
    latest = quality["pass_rate"].iloc[-1] if not quality.empty else None
    head_chip = ""
    if latest is not None and pd.notna(latest):
        head_chip = chip("pass" if latest == 1 else "fail", f"{latest:.0%} latest")
    panel_head(
        "Quality",
        "Share of dbt tests passing in each of the last 20 dbt runs.",
        head_chip,
    )
    if quality.empty:
        st.markdown(EMPTY, unsafe_allow_html=True)
        return
    frame = quality.dropna(subset=["pass_rate"])
    bad = frame[(frame["failed"] > 0) | (frame["warned"] > 0)]
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=frame["generated_at"],
            y=frame["pass_rate"],
            # A line needs two points: with a single dbt run, draw it as a marker.
            mode="lines" if len(frame) > 1 else "markers",
            marker={"size": 8, "color": theme.SERIES},
            line={"color": theme.SERIES, "width": 2, "shape": "linear"},
            customdata=frame[["failed", "warned"]],
            hovertemplate="%{y:.1%} passing · %{customdata[0]} failed · "
            "%{customdata[1]} warned<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=bad["generated_at"],
            y=bad["pass_rate"],
            mode="markers",
            marker={
                "size": 10,
                "symbol": "diamond",
                "color": [theme.CRITICAL if f else theme.WARNING for f in bad["failed"]],
                "line": {"color": theme.PANEL, "width": 2},
            },
            hoverinfo="skip",
        )
    )
    floor = min(0.9, float(frame["pass_rate"].min()) - 0.05) if not frame.empty else 0.9
    layout = theme.chart_layout(height=240)
    lo = max(round(floor, 2), 0)
    layout["yaxis"].update(
        range=[lo, 1.01], tickformat=".0%", tickvals=[round((lo + 1) / 2, 3), 1.0]
    )
    layout["xaxis"].update(
        tickformat="%H:%M",
        showspikes=True,
        spikemode="across",
        spikecolor=theme.MUTED,
        spikethickness=1,
        spikedash="solid",
    )
    fig.update_layout(**layout, hovermode="x")
    plot(fig)
    st.markdown(
        f'<div class="ct-legend">dbt run (wall clock) <i style="{state_var("fail")}">◆</i>'
        f'run with failures<i style="{state_var("warn")}">◆</i>run with warnings</div>',
        unsafe_allow_html=True,
    )


def volume_panel(volume: pd.DataFrame) -> None:
    flagged = volume[volume["is_anomaly"]] if not volume.empty else volume
    head_chip = (
        ""
        if volume.empty
        else chip(
            "warn" if len(flagged) else "pass",
            f"{len(flagged)} flagged" if len(flagged) else "in range",
        )
    )
    panel_head(
        "Volume anomalies",
        "Rows loaded per business day; flagged days sit far outside the same-weekday norm.",
        head_chip,
    )
    if volume.empty:
        st.markdown(EMPTY, unsafe_allow_html=True)
        return
    sources = sorted(volume["source"].unique())
    fig = make_subplots(
        rows=len(sources),
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.16,
        subplot_titles=[f"raw.{s}" for s in sources],
    )
    for row, source in enumerate(sources, start=1):
        series = volume[volume["source"] == source]
        spikes = series[series["is_anomaly"]]
        fig.add_trace(
            go.Scatter(
                x=series["business_date"],
                y=series["row_count"],
                mode="lines",
                line={"color": theme.SERIES, "width": 2},
                fill="tozeroy",
                fillcolor="rgba(57,135,229,0.10)",
                hovertemplate=f"raw.{source} · %{{x|%d %b}}: %{{y:,}} rows<extra></extra>",
            ),
            row=row,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=spikes["business_date"],
                y=spikes["row_count"],
                mode="markers",
                marker={
                    "size": 11,
                    "symbol": "triangle-up",
                    "color": theme.WARNING,
                    "line": {"color": theme.PANEL, "width": 2},
                },
                hovertemplate=f"raw.{source} · %{{x|%d %b}}: %{{y:,}} rows — anomaly"
                "<extra></extra>",
            ),
            row=row,
            col=1,
        )
    layout = theme.chart_layout(height=105 * len(sources) + 30)
    layout["margin"]["t"] = 20
    del layout["xaxis"], layout["yaxis"]
    fig.update_layout(**layout, hovermode="closest")
    axis = theme.chart_layout(0)["xaxis"]
    fig.update_xaxes(**axis, tickformat="%d %b")
    fig.update_yaxes(**axis, tickformat="~s", rangemode="tozero", nticks=3)
    fig.update_annotations(
        font={"color": theme.INK, "size": 11, "family": theme.MONO}, x=0, xanchor="left"
    )
    plot(fig)
    st.markdown(
        f'<div class="ct-legend">rows per business day <i style="{state_var("warn")}">▲</i>'
        "anomaly (|robust z| &gt; 3.5)</div>",
        unsafe_allow_html=True,
    )


NOT_PASSING = ("error", "fail", "warn")


def failed_tests_chip(failed: pd.DataFrame, has_runs: bool) -> str:
    """Counts only fail/warn/error as "not passing", like the KPI tile; skipped tests (blocked by
    an upstream failure) are counted apart."""
    not_passing = int(failed["status"].isin(NOT_PASSING).sum())
    skipped = int((failed["status"] == "skipped").sum())
    skipped_text = f"{skipped} skipped" if skipped else ""
    if not_passing:
        return chip("fail", " · ".join(filter(None, [f"{not_passing} not passing", skipped_text])))
    if skipped:
        return chip("skipped", skipped_text)
    return chip("pass", "all passing") if has_runs else ""


def failed_tests_panel(failed: pd.DataFrame, has_runs: bool) -> None:
    panel_head(
        "Failed tests",
        "dbt tests failing or warning in the latest run, with the rows they caught.",
        failed_tests_chip(failed, has_runs),
    )
    if failed.empty:
        if has_runs:
            note("Every dbt test passed in the latest run.")
        else:
            st.markdown(EMPTY, unsafe_allow_html=True)
        return
    rows = []
    # The query lists errors, failures and warnings first, so skipped tests fill any space left.
    for t in failed.head(7).itertuples():
        skipped = t.status == "skipped"
        failures = "" if pd.isna(t.failures) or not t.failures else f" · {int(t.failures):,} rows"
        meta = "skipped (blocked upstream)" if skipped else f"{t.status}{failures}"
        rows.append(
            f'<div class="ct-row{" skipped" if skipped else ""}" style="{state_var(t.status)}">'
            f'<span class="g">{theme.status_style(t.status)[1]}</span>'
            f'<span class="name" title="{escape(t.test_name)}">{escape(t.test_name)}</span>'
            f'<span class="meta">{escape(meta)}</span>'
            f'<span class="sub">on {escape(readable_node(t.attached_node or "—"))}'
            f"{' — ' + escape(t.message) if isinstance(t.message, str) else ''}</span></div>"
        )
    more = len(failed) - 7
    tail = f'<div class="ct-legend">+{more} more</div>' if more > 0 else ""
    st.markdown(f'<div class="ct-list">{"".join(rows)}</div>{tail}', unsafe_allow_html=True)


SLA_LEVEL = {"met": 0, "pending": 1, "no_data": 1, "breached": 2}


def sla_panel(sla: pd.DataFrame) -> None:
    head_chip = ""
    if not sla.empty:
        breached = int((sla_now(sla)["latest_status"] == "breached").sum())
        head_chip = chip(
            "breached" if breached else "met",
            f"{breached} breached now" if breached else "all met now",
        )
    panel_head(
        "Pipeline SLA",
        "Whether each pipeline promise held, one cell per simulated day (✕ = breached that day).",
        head_chip,
    )
    if sla.empty:
        st.markdown(EMPTY, unsafe_allow_html=True)
        return
    grid = sla.pivot(index="rule", columns="sim_date", values="status")
    levels = grid.apply(lambda col: col.map(SLA_LEVEL))
    glyphs = grid.apply(
        lambda col: col.map(lambda s: theme.status_style(s)[1] if isinstance(s, str) else "")
    )
    met_dim = "#0f5717"  # GOOD at ~45% over the panel: calm when things are fine
    scale = [
        [0, met_dim],
        [0.33, met_dim],
        [0.33, theme.NEUTRAL],
        [0.67, theme.NEUTRAL],
        [0.67, theme.CRITICAL],
        [1, theme.CRITICAL],
    ]
    fig = go.Figure(
        go.Heatmap(
            z=levels.values,
            x=[f"{d:%d}<br>{d:%b}" for d in grid.columns],
            y=list(grid.index),
            text=glyphs.values,
            texttemplate="%{text}",
            textfont={"color": theme.INK, "size": 12},
            customdata=grid.values,
            colorscale=scale,
            zmin=0,
            zmax=2,
            showscale=False,
            xgap=2,
            ygap=2,
            hovertemplate="%{y} · %{x}: %{customdata}<extra></extra>",
        )
    )
    layout = theme.chart_layout(height=36 * len(grid) + 44)
    layout["xaxis"].update(showgrid=False, type="category", tickangle=0)
    layout["yaxis"].update(
        showgrid=False,
        autorange="reversed",
        tickfont={"color": theme.INK, "size": 11, "family": theme.MONO},
    )
    fig.update_layout(**layout)
    plot(fig)
    st.markdown(
        f'<div class="ct-legend"><i style="{state_var("met")}">✓</i>met'
        f'<i style="{state_var("breached")}">✕</i>breached'
        f'<i style="{state_var("pending")}">…</i>pending / no data</div>',
        unsafe_allow_html=True,
    )


def by_impact(incidents: pd.DataFrame) -> pd.DataFrame:
    """Open first, then critical, then the widest blast radius (stable within the query order)."""
    if incidents.empty:
        return incidents
    key = pd.DataFrame(
        {
            "closed": incidents["status"] != "open",
            "minor": incidents["severity"] != "critical",
            "reach": -incidents["affected_downstream"].map(len),
        }
    )
    order = key.sort_values(["closed", "minor", "reach"], kind="stable").index
    return incidents.loc[order].reset_index(drop=True)


def incident_title(inc) -> str:
    return f"{inc.type} · {readable_node(inc.asset)}"


def incidents_panel(incidents: pd.DataFrame, has_data: bool) -> None:
    open_ = incidents[incidents["status"] == "open"] if not incidents.empty else incidents
    head_chip = ""
    if has_data:
        head_chip = chip(
            "critical"
            if (open_["severity"] == "critical").any()
            else "warning"
            if len(open_)
            else "met",
            f"{len(open_)} open",
        )
    panel_head(
        "Data incidents",
        "Every open and recent incident, where it started and how far downstream it reaches.",
        head_chip,
    )
    if incidents.empty:
        if has_data:
            note("No incidents recorded yet.")
        else:
            st.markdown(EMPTY, unsafe_allow_html=True)
        return
    rows = []
    for inc in incidents.head(7).itertuples():
        is_open = inc.status == "open"
        style = inc.severity if is_open else "resolved"
        when = (
            f"open since {short_date(inc.first_seen)}"
            if is_open
            else (f"resolved {short_date(inc.resolved_at)}")
        )
        reach = [readable_node(n) for n in inc.affected_downstream]
        sub = f"reaches {len(reach)}: {', '.join(reach)}" if reach else "no downstream models"
        rows.append(
            f'<div class="ct-row{"" if is_open else " resolved"}" style="{state_var(style)}">'
            f'<span class="g">{theme.status_style(style)[1]}</span>'
            f'<span class="name" title="{escape(inc.asset)}">{escape(incident_title(inc))}</span>'
            f'<span class="meta">{escape(when)}</span>'
            f'<span class="sub">{escape(sub)}</span></div>'
        )
    st.markdown(f'<div class="ct-list">{"".join(rows)}</div>', unsafe_allow_html=True)


# ---------------------------------------------------------------- drill-down

STAGES = (
    ("Staging", ("stg_",)),
    ("Intermediate", ("int_",)),
    ("Marts", ("fct_", "dim_")),
)


def blast_radius(inc, origin: str) -> str:
    """The incident's reach as a left-to-right flow: origin → staging → … → dashboards."""
    nodes = list(inc.affected_downstream)
    columns: list[tuple[str, list[str], str]] = [("Started at", [origin], "origin")]
    models = [readable_node(n) for n in nodes if n.startswith("model.")]
    placed: set[str] = set()
    for stage, prefixes in STAGES:
        names = sorted(m for m in models if m.startswith(prefixes))
        placed.update(names)
        if names:
            columns.append((stage, names, ""))
    other = sorted(set(models) - placed)
    if other:
        columns.append(("Models", other, ""))
    exposures = [readable_node(n) for n in nodes if n.startswith("exposure.")]
    if exposures:
        columns.append(("Dashboards", exposures, "exposure"))
    parts = []
    for i, (heading, names, cls) in enumerate(columns):
        if i:
            parts.append('<div class="ct-arrow">→</div>')
        chips = "".join(f'<div class="ct-node {cls}">{escape(n)}</div>' for n in names)
        parts.append(f'<div class="ct-stage"><div class="h">{heading}</div>{chips}</div>')
    return f'<div class="ct-flow" style="{state_var(inc.severity)}">{"".join(parts)}</div>'


def drill_down(incidents: pd.DataFrame, failed: pd.DataFrame) -> None:
    st.markdown('<div class="ct-section">Incident drill-down</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="ct-caption">Where each open incident started, everything downstream it can '
        "break, and the failing test's SQL.</div>",
        unsafe_allow_html=True,
    )
    open_ = incidents[incidents["status"] == "open"] if not incidents.empty else incidents
    if open_.empty:
        st.caption("No open incidents — nothing to drill into.")
        return
    sql_by_test = {row.test_name: row for row in failed.itertuples()}
    for i, inc in enumerate(open_.itertuples()):
        _, glyph, label = theme.status_style(inc.severity)
        test = sql_by_test.get(inc.asset.split(".")[2]) if inc.asset.startswith("test.") else None
        origin = (
            f"{readable_node(test.attached_node)} (test failing)"
            if test is not None and test.attached_node
            else readable_node(inc.asset)
        )
        reach = len(inc.affected_downstream)
        with st.expander(
            f"{glyph} {label.upper()} · {incident_title(inc)}"
            + (f" · reaches {reach} downstream" if reach else ""),
            expanded=i == 0,
        ):
            st.markdown(
                f'<div class="ct-kv">First seen <b>{escape(sim_label(inc.first_seen))}</b>'
                f" &nbsp;·&nbsp; last seen <b>{escape(sim_label(inc.last_seen))}</b>"
                f" &nbsp;·&nbsp; {escape(inc.detail or '')}</div>",
                unsafe_allow_html=True,
            )
            if reach:
                st.markdown(blast_radius(inc, origin), unsafe_allow_html=True)
            else:
                st.caption("No downstream dbt models — the impact stays at this asset.")
            if test is not None and isinstance(test.compiled_sql, str):
                st.markdown(
                    '<div class="ct-kv">Failed test SQL — run it to see the offending rows</div>',
                    unsafe_allow_html=True,
                )
                st.code(test.compiled_sql.strip(), language="sql", wrap_lines=True)


# ---------------------------------------------------------------- page

dsn = os.environ.get("CONTROL_TOWER_DSN")
if not dsn:
    st.error("CONTROL_TOWER_DSN is not set, so the Control Tower cannot reach the warehouse.")
    st.stop()
try:
    data = load(dsn)
except psycopg.OperationalError as exc:
    st.error(f"Cannot reach the warehouse: {exc}")
    st.stop()

health: HealthSummary = data["health"]
has_data = health.status != "no_data"
header(health)
kpi_strip(data)

top = st.columns(3, gap="small")
bottom = st.columns(3, gap="small")
with top[0].container(border=True, key="panel-freshness"):
    freshness_panel(data["freshness"])
with top[1].container(border=True, key="panel-quality"):
    quality_panel(data["quality"])
with top[2].container(border=True, key="panel-volume"):
    volume_panel(data["volume"])
with bottom[0].container(border=True, key="panel-failed-tests"):
    failed_tests_panel(data["failed"], has_runs=not data["quality"].empty)
with bottom[1].container(border=True, key="panel-sla"):
    sla_panel(data["sla"])
with bottom[2].container(border=True, key="panel-incidents"):
    incidents_panel(by_impact(data["incidents"]), has_data)

drill_down(by_impact(data["incidents"]), data["failed"])
