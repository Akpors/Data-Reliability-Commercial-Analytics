"""Colour tokens, chart defaults and page CSS for the Control Tower's dark ops-console theme.

The status colours are reserved for meaning (good / warning / serious / critical) and always ship
with a glyph and a label, never colour alone. Charts use a single series hue (``SERIES``).
"""

# Surfaces and ink (keep in sync with .streamlit/config.toml next to app.py).
PAGE = "#0b0f14"
PANEL = "#121820"
RAISED = "#18202a"
HAIRLINE = "#243041"
INK = "#e6ebf2"
INK_2 = "#a7b1c0"
MUTED = "#6f7b8c"

# The one series hue (validated dark step of the reference categorical slot 1).
SERIES = "#3987e5"

# Status palette: fixed, never reused for series.
GOOD = "#0ca30c"
WARNING = "#fab219"
SERIOUS = "#ec835a"
CRITICAL = "#d03b3b"
NEUTRAL = "#5b6675"

SANS = "'IBM Plex Sans', system-ui, -apple-system, 'Segoe UI', sans-serif"
MONO = "'IBM Plex Mono', ui-monospace, Menlo, monospace"
DISPLAY = "'Barlow Condensed', 'IBM Plex Sans', sans-serif"

# status -> (colour, glyph, label). Every status the monitoring tables can hold has an entry.
STATUS = {
    "healthy": (GOOD, "●", "Healthy"),
    "degraded": (WARNING, "▲", "Degraded"),
    "critical": (CRITICAL, "◆", "Critical"),
    "no_data": (NEUTRAL, "○", "No data"),
    # freshness
    "pass": (GOOD, "●", "Fresh"),
    "warn": (WARNING, "▲", "Warn"),
    "error": (CRITICAL, "◆", "Error"),
    "runtime error": (CRITICAL, "◆", "Runtime error"),
    # dbt tests
    "fail": (CRITICAL, "◆", "Fail"),
    "skipped": (NEUTRAL, "○", "Skipped"),
    # SLA
    "met": (GOOD, "✓", "Met"),
    "breached": (CRITICAL, "✕", "Breached"),
    "pending": (NEUTRAL, "…", "Pending"),
    # incidents
    "warning": (WARNING, "▲", "Warning"),
    "open": (CRITICAL, "●", "Open"),
    "resolved": (MUTED, "✓", "Resolved"),
}


def status_style(status: str) -> tuple[str, str, str]:
    return STATUS.get(status, (NEUTRAL, "○", status.replace("_", " ").capitalize()))


def chart_layout(height: int) -> dict:
    """Plotly layout shared by every panel: recessive hairline grid, text in ink tokens."""
    axis = {
        "gridcolor": HAIRLINE,
        "gridwidth": 1,
        "linecolor": HAIRLINE,
        "zeroline": False,
        "automargin": True,
        "tickfont": {"color": MUTED, "size": 11, "family": MONO},
        "title": None,
    }
    return {
        "height": height,
        "margin": {"l": 8, "r": 8, "t": 8, "b": 8},
        "paper_bgcolor": PANEL,
        "plot_bgcolor": PANEL,
        "font": {"family": SANS, "color": INK_2, "size": 12},
        "xaxis": dict(axis),
        "yaxis": dict(axis),
        "showlegend": False,
        "hoverlabel": {
            "bgcolor": RAISED,
            "bordercolor": HAIRLINE,
            "font": {"family": SANS, "color": INK, "size": 12},
        },
    }


PLOTLY_CONFIG = {"displayModeBar": False, "responsive": True}

FONTS_URL = (
    "https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@500;600"
    "&family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600&display=swap"
)

CSS = f"""
<style>
@import url('{FONTS_URL}');
:root {{ color-scheme: dark; }}
html, body, .stApp {{ background: {PAGE}; }}
.block-container {{ padding-top: 1.4rem; padding-bottom: 2rem; max-width: 1680px; }}
header[data-testid="stHeader"] {{ background: transparent; height: 0; }}
[data-testid="stToolbar"], [data-testid="stDecoration"] {{ display: none; }}

.ct-top {{ display: flex; align-items: flex-end; justify-content: space-between; gap: 16px;
  flex-wrap: wrap; border-bottom: 1px solid {HAIRLINE}; padding-bottom: 12px; margin-bottom: 14px; }}
.ct-brand {{ font-family: {DISPLAY}; font-weight: 600; font-size: 1.9rem; letter-spacing: .02em;
  color: {INK}; line-height: 1; text-transform: uppercase; }}
.ct-brand small {{ display: block; font-family: {SANS}; font-weight: 400; font-size: .82rem;
  letter-spacing: 0; text-transform: none; color: {INK_2}; margin-top: 6px; }}
.ct-clock {{ text-align: right; font-family: {MONO}; color: {INK}; font-size: 1.05rem; }}
.ct-clock span {{ display: block; font-family: {SANS}; color: {MUTED}; font-size: .72rem;
  text-transform: uppercase; letter-spacing: .12em; margin-bottom: 4px; }}

.ct-strip {{ display: grid; grid-template-columns: 1.6fr repeat(5, 1fr); gap: 2px;
  background: {HAIRLINE}; border: 1px solid {HAIRLINE}; border-radius: 6px; overflow: hidden;
  margin-bottom: 18px; }}
.ct-tile {{ background: {PANEL}; padding: 12px 16px; min-width: 0; }}
.ct-tile .lbl {{ font-size: .72rem; text-transform: uppercase; letter-spacing: .12em; color: {MUTED}; }}
.ct-tile .val {{ font-size: 1.7rem; font-weight: 600; color: {INK}; line-height: 1.25; }}
.ct-tile .sub {{ font-size: .78rem; color: {INK_2}; }}
.ct-tile.hero {{ border-left: 4px solid var(--state); }}
.ct-tile.hero .val {{ font-size: 2.1rem; }}
.ct-tile .glyph {{ color: var(--state); margin-right: 8px; }}
@media (max-width: 900px) {{ .ct-strip {{ grid-template-columns: repeat(2, 1fr); }} }}

[class*="st-key-panel-"] {{ background: {PANEL}; border-radius: 6px; }}
.ct-panel-head {{ display: flex; justify-content: space-between; align-items: baseline; gap: 8px; }}
.ct-panel-title {{ font-family: {DISPLAY}; font-weight: 600; font-size: 1.15rem; color: {INK};
  text-transform: uppercase; letter-spacing: .06em; }}
.ct-caption {{ color: {INK_2}; font-size: .8rem; margin: 2px 0 10px; min-height: 2.3em; }}
.ct-chip {{ font-size: .74rem; color: {INK}; white-space: nowrap; font-family: {MONO}; }}
.ct-chip b {{ color: var(--state); font-weight: 500; margin-right: 4px; }}
.ct-empty {{ border: 1px dashed {HAIRLINE}; border-radius: 6px; color: {MUTED}; padding: 28px 12px;
  text-align: center; font-size: .86rem; }}
.ct-empty code {{ color: {INK_2}; background: {RAISED}; }}
.ct-legend {{ font-size: .74rem; color: {MUTED}; margin-top: 2px; }}
.ct-legend i {{ font-style: normal; color: var(--state); margin: 0 3px 0 10px; }}

.ct-list {{ font-size: .82rem; }}
.ct-row {{ display: grid; grid-template-columns: 18px 1fr auto; gap: 8px; padding: 6px 0;
  border-top: 1px solid {HAIRLINE}; align-items: baseline; }}
.ct-row:first-child {{ border-top: 0; }}
.ct-row .g {{ color: var(--state); }}
.ct-row .name {{ color: {INK}; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
.ct-row .meta {{ color: {MUTED}; font-family: {MONO}; font-size: .74rem; white-space: nowrap; }}
.ct-row .sub {{ grid-column: 2 / 4; color: {INK_2}; font-size: .76rem; overflow: hidden;
  text-overflow: ellipsis; white-space: nowrap; }}
.ct-row.resolved .name {{ color: {INK_2}; }}
.ct-row.skipped .name, .ct-row.skipped .sub {{ color: {MUTED}; }}
.ct-row.skipped .meta {{ font-style: italic; }}

.ct-section {{ font-family: {DISPLAY}; font-weight: 600; font-size: 1.3rem; color: {INK};
  text-transform: uppercase; letter-spacing: .06em; margin: 22px 0 2px; }}
.ct-flow {{ display: flex; align-items: stretch; gap: 0; overflow-x: auto; padding: 4px 0 10px; }}
.ct-stage {{ min-width: 150px; padding: 0 10px; }}
.ct-stage .h {{ font-size: .68rem; text-transform: uppercase; letter-spacing: .12em; color: {MUTED};
  margin-bottom: 6px; }}
.ct-node {{ font-family: {MONO}; font-size: .78rem; color: {INK}; background: {RAISED};
  border: 1px solid {HAIRLINE}; border-radius: 4px; padding: 4px 8px; margin-bottom: 6px;
  white-space: nowrap; }}
.ct-node.origin {{ border-color: var(--state); box-shadow: inset 3px 0 0 var(--state); }}
.ct-node.exposure {{ border-color: {SERIES}; color: {INK}; box-shadow: inset 3px 0 0 {SERIES}; }}
.ct-arrow {{ align-self: center; color: {MUTED}; font-size: 1.1rem; padding-top: 14px; }}
.ct-kv {{ font-size: .82rem; color: {INK_2}; margin-bottom: 8px; }}
.ct-kv b {{ color: {INK}; font-weight: 500; }}
</style>
"""
