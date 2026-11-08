"""
FOLLOW THE MONEY - AML Risk Intelligence dashboard (Streamlit).

Data access follows the Week 5 lab pattern (slide 37): Streamlit sends SQL to Athena with boto3 and only
pulls back small, aggregated results - the heavy lifting stays in Athena / EMR.

  DATA_MODE=athena BUCKET=<bucket> streamlit run dashboard/app.py      # live AWS data
  DATA_MODE=local  streamlit run dashboard/app.py                      # laptop dev on data/analytics CSVs
"""
import glob
import json
import os
import sys
import time

import networkx as nx
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from architecture import architecture_svg  # noqa: E402  (theme-aware pipeline diagram)
from presentation import corridor_preview, ranked_lift_data, route_risk, routes_export  # noqa: E402

MODE = os.environ.get("DATA_MODE", "local")
BUCKET = os.environ.get("BUCKET", "")
REGION = os.environ.get("AWS_REGION", "us-east-1")
ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
LOCAL_DIR = os.environ.get("LOCAL_DIR", os.path.join(ROOT, "data", "analytics"))
ALERTS_DIR = os.path.join(ROOT, "streaming", "out")
DOW = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}

st.set_page_config(page_title="Follow the Money - AML Risk Intelligence", page_icon="🔎", layout="wide")


# ----------------------------------------------------------------------------------------------- data layer
@st.cache_data(ttl=600, show_spinner="Querying Athena...")
def athena_query(sql: str) -> pd.DataFrame:
    import boto3
    ath = boto3.client("athena", region_name=REGION)
    qid = ath.start_query_execution(QueryString=sql, QueryExecutionContext={"Database": "aml"},
                                    ResultConfiguration={"OutputLocation": f"s3://{BUCKET}/athena-results/"}
                                    )["QueryExecutionId"]
    while True:
        s = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if s in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.7)
    if s != "SUCCEEDED":
        raise RuntimeError(f"Athena query {s}: {sql[:120]}")
    pages, cols, rows = ath.get_paginator("get_query_results").paginate(QueryExecutionId=qid), None, []
    for page in pages:
        data = page["ResultSet"]["Rows"]
        if cols is None:
            cols = [c.get("VarCharValue") for c in data[0]["Data"]]
            data = data[1:]
        rows += [[c.get("VarCharValue") for c in r["Data"]] for r in data]
    df = pd.DataFrame(rows, columns=cols)
    for c in df.columns:  # Athena returns strings - convert numeric columns back
        conv = pd.to_numeric(df[c], errors="coerce")
        if conv.notna().sum() == df[c].notna().sum():
            df[c] = conv
    return df


def load(table: str, variant: str) -> pd.DataFrame:
    """Same table names in both modes: aml.<table> in Athena, <table>.csv locally."""
    if MODE == "athena":
        return athena_query(f"SELECT * FROM aml.{table} WHERE variant = '{variant}'")
    path = os.path.join(LOCAL_DIR, f"{table}.csv")
    return pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()


@st.cache_data(ttl=600)
def variants():
    if MODE == "athena":
        try:
            return athena_query("SELECT DISTINCT variant FROM aml.dq_report ORDER BY 1")["variant"].tolist()
        except Exception:
            return ["HI-Small"]
    dq = os.path.join(LOCAL_DIR, "dq_report.csv")
    return pd.read_csv(dq)["variant"].tolist() if os.path.exists(dq) else ["SYN-Small"]


def load_alerts(variant: str):
    """Live alerts: local consumer output if present, else the latest micro-batches in S3."""
    alerts, stats = pd.DataFrame(), {}
    f = os.path.join(ALERTS_DIR, "alerts.jsonl")
    if os.path.exists(f):
        lines = open(f).read().strip().splitlines()[-5000:]
        alerts = pd.DataFrame([json.loads(l) for l in lines if l.strip()])
        sp = os.path.join(ALERTS_DIR, "stats.json")
        stats = json.load(open(sp)) if os.path.exists(sp) else {}
    elif MODE == "athena" and BUCKET:
        alerts = s3_alerts(variant)
    return alerts, stats


def pct(x, d=1):
    return f"{100 * float(x):.{d}f}%"


def csv_button(df: pd.DataFrame, label: str, filename: str, help: str = None, where=None):
    """One small download button; the file name doubles as the widget key so labels can repeat."""
    (where or st).download_button(label, df.to_csv(index=False).encode("utf-8"), file_name=filename,
                                  mime="text/csv", key=f"download_{filename}", help=help)


# ----------------------------------------------------------------------------------------------- chart style
# One style for every chart. Series colours are the first three slots of a validated categorical palette
# (blue, orange, aqua: colour-blind safe as a set, checked against this page's dark and light surfaces).
# Text always uses neutral ink, never a series colour; grids are hairlines; bars are thin with rounded ends.
def ui_mode() -> str:
    t = getattr(getattr(st.context, "theme", None), "type", None)
    return t if t in ("light", "dark") else "dark"


TOKENS = {
    "dark": dict(surface="#0e1117", ink="#f0efec", ink2="#c3c2b7", muted="#898781", grid="#2c2c2a",
                 axis="#383835", hover="#1a1a19", s1="#3987e5", s2="#d95926", s3="#199e70", bad="#d03b3b",
                 cell_text="#0b0b0b", unflagged="#6f6e69",
                 heat=[[0, "#262730"], [0.35, "#5a2d2d"], [0.7, "#b23a36"], [1, "#ff6b6b"]],
                 risk=[[0, "#7a2e2e"], [1, "#ff6b6b"]]),
    "light": dict(surface="#ffffff", ink="#0b0b0b", ink2="#52514e", muted="#898781", grid="#e1e0d9",
                  axis="#c3c2b7", hover="#ffffff", s1="#2a78d6", s2="#eb6834", s3="#1baf7a", bad="#d03b3b",
                  cell_text="#ffffff", unflagged="#c3c2b7",
                  heat=[[0, "#f0efec"], [0.35, "#f2b8b5"], [0.7, "#e05a55"], [1, "#a32424"]],
                  risk=[[0, "#f2a7a7"], [1, "#b52a2a"]]),
}
T = TOKENS[ui_mode()]
FONT = 'system-ui, -apple-system, "Segoe UI", Roboto, sans-serif'


def rgba(hex_colour: str, alpha: float) -> str:
    h = hex_colour.lstrip("#")
    return f"rgba({int(h[0:2], 16)},{int(h[2:4], 16)},{int(h[4:6], 16)},{alpha})"


def style(fig, height: int = 420, grid: bool = True, legend: bool = True):
    """Apply the shared look: transparent surface, neutral ink, hairline grid, legend on top, roomy hover."""
    fig.update_layout(
        height=height, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=FONT, size=13, color=T["ink2"]), title_font=dict(size=15, color=T["ink"]),
        margin=dict(l=8, r=8, t=56 if legend else 24, b=48), showlegend=legend, barcornerradius=4,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, title=None, font=dict(size=13, color=T["ink2"])),
        hoverlabel=dict(bgcolor=T["hover"], bordercolor=T["axis"], font=dict(family=FONT, size=13, color=T["ink"])))
    fig.update_xaxes(showgrid=False, zeroline=False, linecolor=T["axis"], tickcolor=T["axis"], automargin=True,
                     tickfont=dict(color=T["ink2"]), title_font=dict(color=T["muted"], size=12))
    fig.update_yaxes(showgrid=grid, gridcolor=T["grid"], gridwidth=1, zeroline=False, linecolor=T["axis"],
                     automargin=True,
                     tickfont=dict(color=T["ink2"]), title_font=dict(color=T["muted"], size=12))
    return fig


def show(fig, where=None):
    (where or st).plotly_chart(fig, width="stretch", theme=None)


def lift_heatmap(h: pd.DataFrame, x_title: str, label_from: float = None, height: int = 420):
    """Lift on a log scale, one-sided: average or below stays neutral, red builds up to the hotspots.
    The chart's job is to show where risk concentrates, so 'safer than average' is not given a colour."""
    vals = h.astype(float)
    z = np.log10(vals.clip(lower=1.0))                      # <= 1x (average) maps to 0 = neutral
    top = float(np.nanmax(z.values)) if np.isfinite(z.values).any() else 1.0
    top = max(np.log10(2), min(top, 3.0))                   # scale follows the data (hourly lifts are ~2-3x)
    ticks = [float(np.log10(v)) for v in (1, 1.5, 2, 3, 5, 10, 20, 50, 100, 1000) if np.log10(v) <= top + 1e-9]
    if len(ticks) > 5:                                      # keep the colourbar uncluttered on wide ranges
        ticks = [t for t in ticks if abs(t - round(t)) < 1e-9]
    cellmap = vals.map if hasattr(vals, "map") else vals.applymap  # pandas < 2.1 has only applymap
    text = cellmap(lambda v: f"{v:.0f}x" if label_from is not None and pd.notna(v) and v >= label_from else "")
    fig = go.Figure(go.Heatmap(
        z=z.values, x=[str(c) for c in vals.columns], y=list(vals.index), customdata=vals.values,
        colorscale=T["heat"], zmin=0, zmax=top, xgap=2, ygap=2,
        text=text.values, texttemplate="%{text}", textfont=dict(size=12, color="#ffffff"),
        hovertemplate="%{y}, %{x}<br>observed lift %{customdata:.1f}x<extra></extra>",
        colorbar=dict(title=dict(text="observed lift", font=dict(color=T["muted"], size=12)), tickvals=ticks,
                      ticktext=["1x (avg)" if t == 0 else f"{10 ** t:g}x" for t in ticks],
                      tickfont=dict(color=T["ink2"]), outlinewidth=0, thickness=12)))
    fig.update_xaxes(title=x_title, type="category")
    fig.update_yaxes(type="category", autorange="reversed")
    return style(fig, height=height, grid=False, legend=False)


def hbar(labels, values, fmt, x_title: str, height: int = None, x_max: float = None):
    """Single-series horizontal bars, largest on top, value at the tip (one colour: identity is the axis label)."""
    d = pd.DataFrame({"l": list(labels), "v": list(values)}).sort_values("v")
    fig = go.Figure(go.Bar(y=d.l, x=d.v, orientation="h", marker=dict(color=T["s1"]), width=0.55,
                           text=[fmt(v) for v in d.v], textposition="outside", cliponaxis=False, constraintext="none",
                           textfont=dict(color=T["ink"], size=13), hovertemplate="%{y}: %{text}<extra></extra>"))
    fig.update_xaxes(title=x_title, showgrid=True, gridcolor=T["grid"],
                     range=[0, x_max if x_max is not None else float(d.v.max()) * 1.25 or 1])
    fig.update_yaxes(showgrid=False)
    return style(fig, height=height or 60 + 38 * len(d), legend=False)


SIGNAL_NAMES = {"sg_end": "Scatter-gather: start or end of the split", "sg_mule": "Scatter-gather: mule in the middle",
                "cycle": "Money cycle (2-4 hops back to sender)", "fan_out": "Fan-out (pays many in one day)",
                "gather_scatter": "Gather-scatter (collects and pays many, same day)",
                "fan_in": "Fan-in (paid by many in one day)", "segment": "Uses high-risk channels (BQ1)"}


def money(x) -> str:
    x = float(x)
    return f"${x / 1e6:,.1f}M" if x >= 1e6 else f"${x / 1e3:,.0f}K" if x >= 1e3 else f"${x:,.0f}"


def money_md(x) -> str:
    """money() for Streamlit markdown, where two '$' signs would otherwise start a maths formula."""
    return money(x).replace("$", r"\$")


LABEL_SIDES = ["middle right", "top right", "top center", "top left",
               "middle left", "bottom left", "bottom center", "bottom right"]


def _segment_hits_box(seg, b):
    """True if the pixel segment ((x0, y0), (x1, y1)) passes through box b = (x0, y0, x1, y1) (Liang-Barsky)."""
    (x0, y0), (x1, y1) = seg
    dx, dy = x1 - x0, y1 - y0
    t0, t1 = 0.0, 1.0
    for p, q in ((-dx, x0 - b[0]), (dx, b[2] - x0), (-dy, y0 - b[1]), (dy, b[3] - y0)):
        if p == 0:
            if q < 0:
                return False
        else:
            t = q / p
            if p < 0:
                t0 = max(t0, t)
            else:
                t1 = min(t1, t)
            if t0 > t1:
                return False
    return True


def place_labels(items, obstacles, to_px, gap=3, segments=()):
    """Greedy label placement without overlaps, in pixels.

    items: (node, (x, y), marker_radius_px, font_size, preferred_side, [text, shorter fallback, ...]) in priority
    order. obstacles: pixel boxes (x0, y0, x1, y1) that labels must avoid (markers, the 'selected' chip).
    segments: (weight, ((x0, y0), (x1, y1))) pixel lines (the arrows) that labels should avoid where they can.
    Each label tries its texts in turn and, for each, the 8 positions around its dot; a spot may not hit a box,
    and among the free spots the one crossing the fewest (weighted) arrows wins, ties going to the preferred
    (outward) side. Returns {node: (side, text)}, text '' = hidden."""
    import re as _re
    taken = list(obstacles)

    def box(px, py, r, w, h, side):
        v, hz = side.split()
        x0 = px - w / 2 if hz == "center" else px + r + 2 if hz == "right" else px - r - 2 - w
        y0 = py - h / 2 if v == "middle" else py - r - 2 - h if v == "top" else py + r + 2
        return (x0, y0, x0 + w, y0 + h)

    def hits(b, pool):
        return any(b[0] < o[2] + gap and o[0] < b[2] + gap and b[1] < o[3] + gap and o[1] < b[3] + gap for o in pool)

    def crossings(b):
        return sum(wt for wt, seg in segments if _segment_hits_box(seg, b))

    out = {}
    for node, (x, y), r, size, pref, texts in items:
        px, py = to_px(x, y)
        own = (px - r, py - r, px + r, py + r)
        i0 = LABEL_SIDES.index(pref) if pref in LABEL_SIDES else 0
        order = [LABEL_SIDES[(i0 + d) % 8] for d in (0, 1, -1, 2, -2, 3, -3, 4)]
        out[node] = (pref, "")
        for text in texts:
            others = [o for o in taken if o != own]        # a label may touch its own dot, nothing else
            lines = [_re.sub(r"<[^>]+>", "", s) for s in text.split("<br>")]
            w, h = max(len(s) for s in lines) * size * 0.58 + 4, len(lines) * size * 1.25 + 2
            free = [(crossings(box(px, py, r, w, h, s)), k, s) for k, s in enumerate(order)
                    if not hits(box(px, py, r, w, h, s), others)]
            if free:
                spot = min(free)[2]
                taken.append(box(px, py, r, w, h, spot))
                out[node] = (spot, text)
                break
    return out


def label_annotation(x, y, side, text, r, size, colour, bg):
    """A node label as a plotly annotation at the spot place_labels chose: same offsets as its pixel box, with a
    light background so an arrow that still has to pass under the label does not strike through the text."""
    v, hz = side.split()
    return dict(x=x, y=y, xref="x", yref="y", showarrow=False, text=text,
                xanchor={"right": "left", "left": "right"}.get(hz, "center"),
                yanchor={"top": "bottom", "bottom": "top"}.get(v, "middle"),
                xshift=(r + 2) if hz == "right" else -(r + 2) if hz == "left" else 0,
                yshift=(r + 2) if v == "top" else -(r + 2) if v == "bottom" else 0,
                align={"right": "left", "left": "right"}.get(hz, "center"),
                font=dict(size=size, color=colour), bgcolor=bg, borderpad=1)


def flag_tags(r, limit: int = 3) -> str:
    """Short version of an account's red flags, e.g. '28 cycles, splits funds, mule'."""
    why = str(getattr(r, "reasons", "") or "")
    tags = []
    if "money cycle" in why:
        n = int(getattr(r, "n_cycles", 0) or 0)
        tags.append(f"{n} cycle{'s' if n != 1 else ''}" if n else "cycles")
    if "scatter-gather source" in why:
        tags.append("splits funds")
    if "scatter-gather sink" in why:
        tags.append("collects funds")
    if "(mule)" in why:
        tags.append("mule")
    if "(fan-out)" in why:
        tags.append(f"pays {int(getattr(r, 'max_daily_fan_out', 0) or 0)} in a day")
    if "(fan-in)" in why:
        tags.append(f"paid by {int(getattr(r, 'max_daily_fan_in', 0) or 0)} in a day")
    if "gather-scatter" in why:
        tags.append("gather-scatter")
    if "hub account" in why:
        tags.append("busy hub")
    if "high-risk channels" in why:
        tags.append("risky channels")
    return ", ".join(tags[:limit]) + (f" +{len(tags) - limit} more" if len(tags) > limit else "")


def precision_chart(df: pd.DataFrame, series, title: str, ytitle: str) -> go.Figure:
    """Precision per watchlist size. k is a category ('Top 100'), so the groups are evenly spaced. Only the
    headline series carries value labels; random selection is a reference line, not a fourth bar."""
    df = df.sort_values("k")
    cats = [f"Top {int(k):,}" for k in df.k]
    colours = [T["s1"], T["s2"], T["s3"]]
    fig = go.Figure()
    for i, (col, name) in enumerate(c for c in series if c[0] != "base_rate"):
        vals = df[col] * 100
        lab = [f"{v:.1f}".rstrip("0").rstrip(".") + "%" for v in vals]
        fig.add_trace(go.Bar(x=cats, y=vals, name=name, marker=dict(color=colours[i]), customdata=lab,
                             text=lab if i == 0 else None, textposition="outside", cliponaxis=False, constraintext="none",
                             textfont=dict(size=14, color=T["ink"]),
                             hovertemplate=f"{name}<br>%{{x}}: %{{customdata}}<extra></extra>"))
    top = float(max((df[c] * 100).max() for c, _ in series))
    fig.update_layout(barmode="group", bargap=0.35, bargroupgap=0.12,
                      title=dict(text=title, x=0, xanchor="left", y=0.98, yanchor="top"))
    if "base_rate" in df:
        base = float(df.base_rate.iloc[0]) * 100
        fig.add_hline(y=base, line=dict(color=T["muted"], width=1.5))
        fig.add_trace(go.Scatter(x=[None], y=[None], mode="lines", line=dict(color=T["muted"], width=1.5),
                                 name=f"Random selection ({base:.1f}%)", hoverinfo="skip"))
    fig.update_xaxes(type="category", title=None, tickfont=dict(size=13, color=T["ink"]))
    fig.update_yaxes(title=ytitle, range=[0, top * 1.18 + 1], ticksuffix="%")
    fig = style(fig, height=460)
    fig.update_layout(margin=dict(t=96, b=70), legend=dict(y=1.0, yanchor="bottom"))
    return fig


@st.cache_data(ttl=30, show_spinner=False)
def s3_alerts(variant: str):
    import boto3
    s3 = boto3.client("s3", region_name=REGION)
    objs = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"streaming/alerts/variant={variant}/").get("Contents", [])
    rows = []
    for o in sorted(objs, key=lambda o: o["LastModified"])[-20:]:
        body = s3.get_object(Bucket=BUCKET, Key=o["Key"])["Body"].read().decode()
        rows += [json.loads(l) for l in body.splitlines() if l.strip()]
    return pd.DataFrame(rows)


@st.cache_data(ttl=600, show_spinner="Reading the pipeline's query log...")
def run_logs() -> pd.DataFrame:
    import boto3
    s3 = boto3.client("s3", region_name=REGION)
    logs = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="analytics/run_log/"):
        for o in page.get("Contents", []):
            body = s3.get_object(Bucket=BUCKET, Key=o["Key"])["Body"].read().decode()
            logs += [json.loads(l) for l in body.splitlines() if l.strip()]
    return pd.DataFrame(logs)


# ----------------------------------------------------------------------------------------------- layout
st.sidebar.title("🔎 Follow the Money")
st.sidebar.caption("IS459 G2: AML Risk Intelligence on AWS")
variant = st.sidebar.selectbox("Dataset variant", variants())
st.sidebar.markdown(f"**Data source:** `{MODE}`" + (f", `s3://{BUCKET}`" if MODE == "athena" else ""))
if MODE == "local":
    st.sidebar.warning("Local mode shows SYNTHETIC test data - not real findings.")
st.sidebar.caption("Tip: every chart and table has a full-screen button in its top-right corner.")
st.sidebar.markdown("---\n**Pipeline:** S3 → Glue ETL → Glue Catalog → Athena (BQ1) → EMR Spark (BQ2) → "
                    "Kafka live monitor → this dashboard. Orchestrated by Step Functions.")

dq = load("dq_report", variant)
tab_overview, tab_q1, tab_q2, tab_live, tab_ops = st.tabs(
    ["Overview", "BQ1: Risk hotspots", "BQ2: Laundering networks", "Live monitor", "Pipeline & cost"])

# ----------------------------------------------------------------------------------------------- overview
with tab_overview:
    st.header("Where is the dirty money, and who is moving it?")
    st.write("Two questions, one pipeline: **BQ1** tells the monitoring team *which channels* to watch; "
             "**BQ2** tells investigators *which accounts* form laundering networks, and why.")
    if not dq.empty:
        r = dq.iloc[0]
        c = st.columns(4)
        c[0].metric("Transactions processed", f"{int(r.rows_out):,}")
        c[1].metric("Laundering transactions", f"{int(r.laundering_rows):,}",
                    f"1 in {int(r.rows_out / max(r.laundering_rows, 1)):,}", delta_color="off")
        c[2].metric("Rows dropped by data-quality checks", f"{int(r.rows_malformed_dropped + r.rows_duplicate_dropped):,}")
        c[3].metric("Rows without FX rate", f"{int(r.rows_without_fx_rate):,}")
    ev = load("q2_evaluation", variant)
    evt = load("q2_evaluation_time", variant)
    seg = load("q1_segment_risk", variant)
    # Headline BQ2 figure = the OFFICIAL time-based grade (docs/decisions.md); same-days only as a fallback
    official = (evt[evt.period == "official"].sort_values("k")
                if not evt.empty and (evt.n_holdout_bad > 0).any() else pd.DataFrame())
    if not seg.empty and (not official.empty or not ev.empty):
        e = official if not official.empty else ev.sort_values("k")
        k100 = e[e.k == 100].iloc[0] if (e.k == 100).any() else e.iloc[0]
        cap = seg.sort_values("risk_rank")
        cap5 = cap[cap.cum_txn_share <= 0.05].cum_laundering_share.max()
        c = st.columns(3)
        c[0].metric("BQ1: laundering caught by reviewing the riskiest 5%", pct(cap5 if pd.notna(cap5) else 0),
                    help="Channel risk is learned on the learning days and measured on those same days.")
        if not official.empty:
            c[1].metric("BQ2: top-100 watchlist precision (official)",
                        pct(k100.precision_combined),
                        f"{k100.precision_combined / max(k100.base_rate, 1e-9):.0f}x better than random",
                        delta_color="normal",
                        help="Flagged using only the learning days, graded on the following days, on the half of the "
                             "accounts whose labels were never used to build the scorecard.")
            c[2].metric("Graded accounts that launder later",
                        f"{int(k100.n_holdout_bad):,} of {int(k100.n_holdout_accounts):,}",
                        f"{pct(k100.base_rate)} = picking at random", delta_color="off")
        else:
            c[1].metric("Precision of top-100 watchlist (BQ2, same days, unseen accounts)", pct(k100.precision_combined),
                        f"{k100.precision_combined / max(k100.base_rate, 1e-9):.0f}x better than random", delta_color="normal")
            c[2].metric("Laundering accounts in holdout set", f"{int(k100.n_holdout_bad):,} of {int(k100.n_holdout_accounts):,}")

# ----------------------------------------------------------------------------------------------- BQ1
with tab_q1:
    st.header("BQ1: Where should the monitoring team spend its limited review capacity?")
    if seg.empty:
        st.info("Run the pipeline to populate BQ1 results.")
    else:
        seg = seg.sort_values("risk_rank")
        with st.container(border=True):
            st.subheader("Alert-budget optimiser")
            budget = st.slider("Share of all transactions your team can review", 0.5, 20.0, 5.0, 0.5,
                               format="%.1f%%") / 100
            chosen = seg[seg.cum_txn_share <= budget]
            caught = chosen.cum_laundering_share.max() if not chosen.empty else 0.0
            c = st.columns(3)
            c[0].metric("Laundering transactions caught", pct(caught))
            c[1].metric("Segments to monitor", f"{len(chosen):,}")
            c[2].metric("Efficiency vs. random review", f"{caught / budget:.1f}x" if budget else "-")
            curve = pd.concat([pd.DataFrame({"cum_txn_share": [0.0], "cum_laundering_share": [0.0]}),
                               seg[["cum_txn_share", "cum_laundering_share"]]])
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=[0, 100], y=[0, 100], mode="lines", name="Random review",
                                     line=dict(color=T["muted"], width=1.5), hoverinfo="skip"))
            fig.add_trace(go.Scatter(x=curve.cum_txn_share * 100, y=curve.cum_laundering_share * 100, mode="lines",
                                     name="Risk-ranked review", line=dict(color=T["s1"], width=2.5),
                                     fill="tozeroy", fillcolor=rgba(T["s1"], 0.10),
                                     hovertemplate="Review %{x:.1f}% of transfers<br>catch %{y:.1f}% of laundering"
                                                   "<extra></extra>"))
            fig.add_vline(x=budget * 100, line=dict(color=T["muted"], width=1))
            used = float(chosen.cum_txn_share.max()) if not chosen.empty else 0.0
            fig.add_trace(go.Scatter(x=[used * 100], y=[caught * 100], mode="markers+text", showlegend=False,
                                     marker=dict(size=12, color=T["s1"], line=dict(width=2, color=T["surface"])),
                                     text=[f"<b>{caught * 100:.0f}% caught</b> by reviewing {used * 100:.1f}%"],
                                     textposition="middle right", textfont=dict(size=14, color=T["ink"]),
                                     hoverinfo="skip"))
            fig.update_xaxes(title="share of transfers reviewed", range=[0, 30], ticksuffix="%")
            fig.update_yaxes(title="share of laundering caught", range=[0, 102], ticksuffix="%")
            show(style(fig, height=400))
            n_rules = len(chosen)
            st.subheader(f"Monitoring rules at this budget · {n_rules:,} segment{'s' if n_rules != 1 else ''}")
            st.caption("Every segment inside the budget, ranked by the 95% Wilson lower bound of the laundering "
                       "rate, so small lucky segments cannot top the list. The lift column shows observed lift.")
            rules = chosen.assign(
                corridor=lambda d: d.is_cross_bank.map({1: "cross-bank", 0: "same bank"}),
                fx=lambda d: d.is_cross_currency.map({1: "cross-currency", 0: "same currency"}),
                rate=lambda d: (d.laundering_rate * 100).round(2), lift=lambda d: d.lift.round(1))
            st.dataframe(rules[["risk_rank", "payment_format", "payment_currency", "corridor", "fx", "amount_band",
                                "n_txn", "n_laundering", "rate", "lift"]], hide_index=True, width="stretch",
                         column_config={"rate": st.column_config.NumberColumn("laundering rate", format="%.2f%%"),
                                        "lift": st.column_config.NumberColumn("observed lift", format="%.1fx", help="Observed laundering rate divided by the overall rate."),
                                        "risk_rank": st.column_config.NumberColumn("rank"),
                                        "payment_format": "payment", "payment_currency": "currency",
                                        "amount_band": "amount (USD)",
                                        "n_txn": st.column_config.NumberColumn("transfers", format="%d"),
                                        "n_laundering": st.column_config.NumberColumn("laundering", format="%d")})

        st.subheader("Risk patterns across all transfers")
        st.caption("Everything below covers the whole dataset and does not change with the review budget above. "
                   "Observed lift = observed laundering rate / overall rate. Conservative lift uses the 95% Wilson "
                   "lower bound of that rate / overall rate, reducing the influence of small samples.")

        fc = load("q1_format_currency", variant)
        tp = load("q1_time_profile", variant)
        c1, c2 = st.columns(2)
        if not fc.empty:
            h = fc[fc.n_txn >= 30].pivot_table(index="payment_currency", columns="payment_format", values="lift")
            c1.subheader("Observed lift: payment format and currency")
            show(lift_heatmap(h, "payment format", label_from=10, height=460), c1)
            c1.caption("Red marks combinations riskier than average (observed lift above 1x); numbers mark the hotspots "
                       "(10x or more). Combinations with fewer than 30 transfers are left blank.")
            csv_button(ranked_lift_data(fc[fc.n_txn >= 30], ["payment_format", "payment_currency"]),
                       "Download payment / currency groups (CSV)", f"{variant}_payment_currency_by_lift.csv", where=c1,
                       help="Every combination shown above, ranked by observed lift (CSV column: lift), with exact counts and rates.")
        if not tp.empty:
            t = tp.assign(day=tp.day_of_week.map(DOW)).pivot_table(index="day", columns="txn_hour", values="lift")
            t = t.reindex([d for d in DOW.values() if d in t.index])
            c2.subheader("When does laundering happen? (observed lift by hour)")
            show(lift_heatmap(t, "hour of day", height=460), c2)
            wk = tp.day_of_week >= 6
            rate = lambda m: tp[m].n_laundering.sum() / max(tp[m].n_txn.sum(), 1)
            ratio = rate(wk) / max(rate(~wk), 1e-12)
            hot = tp[tp.n_txn >= 30].sort_values("lift", ascending=False).head(3)
            peaks = ", ".join(f"{DOW[int(r.day_of_week)]} {int(r.txn_hour):02d}:00 ({r.lift:.1f}x)"
                              for r in hot.itertuples())
            c2.caption(f"Weekend transfers are **{ratio:.1f}x** as likely to be laundering as weekday ones. "
                       f"Riskiest hours: {peaks}. Hover a cell for its observed lift.")
            csv_button(ranked_lift_data(tp.assign(day=tp.day_of_week.map(DOW)), ["day_of_week", "day", "txn_hour"]),
                       "Download day / hour slots (CSV)", f"{variant}_day_hour_by_lift.csv", where=c2,
                       help="Every day-and-hour slot, ranked by observed lift (CSV column: lift), with exact counts and rates.")
        bc = load("q1_bank_corridors", variant)
        if not bc.empty:
            bc = route_risk(bc.assign(from_bank=bc.from_bank.astype(str), to_bank=bc.to_bank.astype(str)))
            st.subheader("Bank-to-bank routes")
            st.caption(f"{len(bc):,} cross-bank routes with at least 50 transfers and at least one laundering "
                       "transfer. Routes below that bar are left out, so a missing route does not mean no activity.")
            sent = bc.groupby("from_bank").laundered_usd.sum().sort_values(ascending=False)
            senders = sent.index.tolist()
            sender_key = f"corridor_sender_{variant}"
            route_chart_key = f"corridor_chart_{variant}"
            route_notice_key = f"corridor_notice_{variant}"
            heat_key = f"corridor_heatmap_{variant}"
            ROUTE_TABS = ["Heatmap: observed lift between the busiest banks", "Routes from one bank: conservative lift",
                          "Routes moving the most laundered money"]
            tabs_key = f"corridor_tabs_{variant}"
            # keyed tabs (Streamlit 1.65+) remember the open tab, so a heatmap click can switch to the route chart
            t_heat, t_risk, t_value = st.tabs(ROUTE_TABS, key=tabs_key, on_change="rerun")
            with t_heat:
                # sender x receiver lift for the 20 banks sending / receiving the most laundered value; a full
                # bank-by-bank matrix would be thousands wide on HI-Large and almost entirely empty
                N = 20
                top_s = senders[:N]
                top_r = bc.groupby("to_bank").laundered_usd.sum().sort_values(ascending=False).index[:N].tolist()
                cells = bc[bc.from_bank.isin(top_s) & bc.to_bank.isin(top_r)]

                def open_heat_sender():
                    """Clicking a cell selects its sender bank in the 'Routes from one bank: conservative lift' tab."""
                    points = st.session_state.get(heat_key, {}).get("selection", {}).get("points", [])
                    bank = next((p.get("customdata") for p in reversed(points)
                                 if isinstance(p.get("customdata"), str)), None)
                    if bank is not None:
                        if bank in sent.index:
                            st.session_state[sender_key] = bank
                            st.session_state[tabs_key] = ROUTE_TABS[1]
                            st.session_state[route_notice_key] = (
                                f"Showing bank {bank}'s routes, ranked by conservative lift (95% Wilson lower bound).")

                if cells.empty:
                    st.info("None of the busiest banks share a qualifying route.")
                else:
                    h = (cells.assign(sender=[f"from bank {b}" for b in cells.from_bank],
                                      receiver=[f"to {b}" for b in cells.to_bank])
                         .pivot_table(index="sender", columns="receiver", values="lift")
                         .reindex(index=[f"from bank {b}" for b in top_s], columns=[f"to {b}" for b in top_r]))
                    fig = lift_heatmap(h, "receiver bank", label_from=10, height=min(720, 160 + 24 * len(h)))
                    # Streamlit reports clicks on markers, not on heatmap cells: an invisible square on every filled
                    # cell carries the sender bank (and the hover text, so hovering still shows the route)
                    fig.add_trace(go.Scatter(
                        x=[f"to {b}" for b in cells.to_bank], y=[f"from bank {b}" for b in cells.from_bank],
                        mode="markers", marker=dict(symbol="square", size=24, color="rgba(0,0,0,0)"),
                        customdata=cells.from_bank.tolist(), showlegend=False, hoverinfo="text",
                        hovertext=[f"Bank {r.from_bank} → bank {r.to_bank}<br>observed lift {r.lift:.1f}x<br>"
                                   f"{int(r.n_txn):,} transfers, {int(r.n_laundering):,} laundering<br>"
                                   f"laundered {money(r.laundered_usd)}<br><i>click to pick bank {r.from_bank} "
                                   "in the next tab</i>" for r in cells.itertuples()]))
                    fig.update_layout(clickmode="event+select")
                    st.plotly_chart(fig, width="stretch", theme=None, key=heat_key,
                                    on_select=open_heat_sender, selection_mode="points")
                    share = cells.laundered_usd.sum() / max(bc.laundered_usd.sum(), 1e-9)
                    st.caption(f"Banks are chosen by volume, colour shows observed lift: rows are the {len(top_s)} banks sending "
                               f"the most laundered value, columns the {len(top_r)} receiving the most, and each cell is "
                               f"one route's observed lift (numbers mark 10x or more). These {len(cells):,} routes carry "
                               f"**{share:.0%} of the laundered value** on qualifying routes; pick any bank in the next "
                               "tab for the rest. Blank means that route is below the bar above, not that no money "
                               "moved. Click a cell to see all routes from its sender bank.")
                    csv_button(routes_export(cells), f"Download {len(cells):,} heatmap routes (CSV)",
                               f"{variant}_bank_routes_heatmap.csv",
                               help="The routes in this grid, ranked by conservative lift. CSV: lift = observed lift; lift_at_95pct_lower_bound = conservative lift.")
            with t_risk:

                def open_receiver_as_sender():
                    """Clicking a receiver's bar makes that bank the sender, or says why it cannot."""
                    points = st.session_state.get(route_chart_key, {}).get("selection", {}).get("points", [])
                    bank = next((p.get("customdata") for p in reversed(points)
                                 if isinstance(p.get("customdata"), str)), None)
                    if bank in sent.index:
                        st.session_state[sender_key] = bank
                        st.session_state[route_notice_key] = (
                            f"Showing bank {bank}'s routes, ranked by conservative lift (95% Wilson lower bound).")
                    elif bank is not None:
                        st.session_state[route_notice_key] = (
                            f"Bank {bank} sends on no route that meets the bar above, so there is nothing to open. "
                            "The sender has not changed.")

                notice = st.session_state.pop(route_notice_key, None)
                if notice:
                    st.info(notice)
                sender = st.selectbox("Sender bank", senders, key=sender_key,
                                      format_func=lambda b: f"Bank {b} · {money(sent[b])} laundered sent",
                                      help="Banks are listed by laundered value sent; type to search for a bank id.")
                routes, n_routes = corridor_preview(bc, sender, max_routes=15)
                ylabels = [f"to bank {b}" for b in routes.to_bank]
                # bar = lift at the 95% Wilson lower bound, so bar length and order agree and a route with one
                # lucky laundering transfer cannot top the list (same ranking idea as the monitoring rules)
                fig = go.Figure(go.Bar(
                    y=ylabels, x=routes.lb_lift, orientation="h", marker=dict(color=T["s1"]), width=0.6,
                    customdata=routes.to_bank.tolist(),
                    text=[f"conservative {r.lb_lift:.1f}x | observed {r.lift:.1f}x | {int(r.n_txn):,} transfers"
                          for r in routes.itertuples()],
                    textposition="outside", cliponaxis=False, constraintext="none",
                    textfont=dict(color=T["ink"], size=13),
                    hovertext=[f"Bank {sender} → bank {r.to_bank}<br>observed lift {r.lift:.1f}x; conservative lift "
                               f"{r.lb_lift:.1f}x (95% Wilson lower bound)<br>{int(r.n_txn):,} transfers, "
                               f"{int(r.n_laundering):,} laundering ({r.laundering_rate * 100:.2f}%)<br>"
                               f"laundered {money(r.laundered_usd)}<br><i>click to open bank {r.to_bank} as sender</i>"
                               for r in routes.itertuples()], hoverinfo="text"))
                fig.update_xaxes(title="conservative lift (95% Wilson lower bound)",
                                 showgrid=True, gridcolor=T["grid"], range=[0, float(routes.lb_lift.max()) * 1.9 or 1],
                                 ticksuffix="x")
                fig.update_yaxes(type="category", autorange="reversed", showgrid=False)
                fig.update_layout(clickmode="event+select")
                st.plotly_chart(style(fig, height=90 + 34 * len(routes), legend=False), width="stretch", theme=None,
                                key=route_chart_key, on_select=open_receiver_as_sender, selection_mode="points")
                st.caption(f"{'All' if n_routes <= 15 else f'Top {len(routes)} of'} {n_routes:,} route"
                           f"{'s' if n_routes != 1 else ''} from bank {sender}, ranked by how confident we can be of their risk "
                           "(95% Wilson lower bound, as for the monitoring rules), so a route with only a few transfers "
                           "cannot top the list on one lucky case. Bar length shows conservative lift; labels also show "
                           "observed lift. Click a bar to follow the money to that bank.")
                csv_button(routes_export(bc[bc.from_bank == sender]),
                           f"Download all {n_routes:,} routes from bank {sender} (CSV)",
                           f"{variant}_routes_from_bank_{sender}.csv",
                           help="All qualifying routes from the selected sender, including those beyond the 15 shown; "
                                "ranked by conservative lift within this bank. CSV: lift = observed lift; "
                                "lift_at_95pct_lower_bound = conservative lift (95% Wilson lower bound).")
            with t_value:
                top = bc.sort_values("laundered_usd", ascending=False)
                st.dataframe(top.head(15).assign(rate=top.laundering_rate * 100)[
                    ["from_bank", "to_bank", "n_txn", "n_laundering", "usd_volume", "laundered_usd", "rate", "lift"]],
                    hide_index=True, width="stretch",
                    column_config={"from_bank": st.column_config.TextColumn("from bank"),
                                   "to_bank": st.column_config.TextColumn("to bank"),
                                   "n_txn": st.column_config.NumberColumn("transfers", format="%d"),
                                   "n_laundering": st.column_config.NumberColumn("laundering", format="%d"),
                                   "usd_volume": st.column_config.NumberColumn("total value (USD)", format="compact"),
                                   "laundered_usd": st.column_config.NumberColumn("laundered (USD)", format="compact"),
                                   "rate": st.column_config.NumberColumn("laundering rate", format="%.2f%%"),
                                   "lift": st.column_config.NumberColumn("observed lift", format="%.1fx", help="Observed laundering rate divided by the overall rate.")})
                st.caption(f"Top 15 of {len(top):,} routes by laundered value.")
                csv_button(routes_export(bc, by="value"), f"Download all {len(top):,} bank routes by value (CSV)",
                           f"{variant}_bank_routes_all.csv",
                           help="Every qualifying route, most laundered value first. CSV: lift = observed lift; "
                                "lift_at_95pct_lower_bound = conservative lift; rank_by_risk ranks conservative lift.")

# ----------------------------------------------------------------------------------------------- BQ2
with tab_q2:
    st.header("BQ2: Which accounts form laundering networks, and why?")
    wl = load("q2_watchlist", variant)
    if wl.empty:
        st.info("Run the pipeline to populate BQ2 results.")
    else:
        wl = wl.sort_values("risk_rank")
        series = [("precision_combined", "Combined scorecard (BQ1 + BQ2)"),
                  ("precision_network_only", "Network motifs only"),
                  ("precision_segment_only", "Channel rules only (BQ1)"),
                  ("base_rate", "Random selection")]
        HALF = {"holdout": "graded (labels never used)", "calibration": "learning (labels set the points)"}
        has_split = "split" in wl.columns
        with st.container(border=True):
            st.subheader("Investigator watchlist")
            st.caption("The search and filter apply to this table and to the money-trail account picker below. "
                       "Click a row to open that account's money trail.")
            f1, f2 = st.columns([3, 2])
            with f1:
                q = st.text_input("Filter by account id or reason (e.g. 'cycle', 'mule')", "")
            view = wl[wl.acct.str.contains(q, case=False, regex=False)
                      | wl.reasons.fillna("").str.contains(q, case=False, regex=False)] if q else wl
            with f2:
                st.write("")
                graded_only = has_split and st.toggle("Only the graded half (accounts whose labels were never used)",
                                                      value=False)
            if graded_only:
                view = view[view["split"] == "holdout"]
            # money trails are published for the top 300 accounts; the picker follows the watchlist filter
            filtered = view[view.risk_rank <= 300].acct.tolist()
            options = filtered or wl.acct.head(300).tolist()
            trail_accounts = set(wl[wl.risk_rank <= 300].acct)
            picker_key = f"trail_account_{variant}"
            chart_key = f"trail_graph_{variant}"
            notice_key = f"trail_notice_{variant}"
            # The table's tick is remembered by ROW POSITION and Streamlit cannot set it from code, so whenever the
            # account changes other than by clicking a row (filter, search, dropdown, graph) the tick is cleared by
            # giving the table a fresh key. Otherwise the tick would sit on whatever account now fills that row.
            nonce_key, sig_key = f"watchlist_table_nonce_{variant}", f"watchlist_filter_sig_{variant}"

            def clear_tick():
                st.session_state[nonce_key] = st.session_state.get(nonce_key, 0) + 1

            if st.session_state.get(sig_key) != (q, graded_only):
                st.session_state[sig_key] = (q, graded_only)
                clear_tick()
            table_key = f"watchlist_table_{variant}_{st.session_state[nonce_key]}"

            def open_trail(node, from_table=False):
                """Open a clicked account's trail, or say why it cannot be opened."""
                if node in options:
                    if not from_table:
                        clear_tick()
                    st.session_state[picker_key] = node
                    st.session_state[notice_key] = (f"Opened money trail for {node}. The graph and account "
                                                    "reasons now show this account.")
                elif node in trail_accounts:
                    st.session_state[notice_key] = (
                        f"{node} does not match the current watchlist search or graded-half filter. Clear the filter to open its trail. "
                        "The selected account has not changed.")
                else:
                    st.session_state[notice_key] = (
                        f"{node} is outside the top-300 trail coverage, so a separate money trail is not available. "
                        "The selected account has not changed.")

            def follow_table_row():
                rows = st.session_state.get(table_key, {}).get("selection", {}).get("rows", [])
                if rows and rows[0] < len(shown):
                    open_trail(shown.iloc[rows[0]].acct, from_table=True)

            def follow_trail_node():
                """Clicking a node on the graph opens that account's trail (or says why it cannot)."""
                points = st.session_state.get(chart_key, {}).get("selection", {}).get("points", [])
                for point in reversed(points):
                    if isinstance(point.get("customdata"), str):
                        open_trail(point["customdata"])
                        break

            # table cells cannot wrap, so the table shows the short form of the red flags; the full sentences
            # appear as bullet points for the selected account in the money-trail panel below
            cols = ["risk_rank", "acct", "risk"] + (["half"] if has_split else []) + ["red_flags"]
            shown = view                                # all 1,000 accounts; the table scrolls
            st.dataframe(shown.assign(risk=(shown.risk_probability * 100).round(1),
                                      half=shown["split"].map(HALF) if has_split else "",
                                      red_flags=[flag_tags(r, limit=9) or "channel risk only" for r in shown.itertuples()]
                                      )[cols],
                         hide_index=True, width="stretch", height=320, key=table_key,
                         on_select=follow_table_row, selection_mode="single-row",
                         column_config={"risk_rank": st.column_config.NumberColumn("rank"),
                                        "acct": st.column_config.TextColumn("account"),
                                        "risk": st.column_config.ProgressColumn("risk score", format="%.1f",
                                                                                min_value=0, max_value=100),
                                        "half": st.column_config.TextColumn("account half"),
                                        "red_flags": st.column_config.TextColumn(
                                            "red flags", help="Short form. Click the row to read each reason in "
                                                              "full in the money-trail explorer below.")})
            st.caption(f"{len(view):,} of {len(wl):,} watchlist accounts match"
                       + ". Money trails are available for the top 300. Scores use only each account's own money-flow patterns; the points per red flag were "
                         "learned from the labels of the learning half, on the learning days.")

            st.subheader("Money-trail explorer")
            notice = st.session_state.pop(notice_key, None)
            if notice:
                st.info(notice)
            if not filtered:
                st.caption("No top-300 account matches the watchlist search or filter, so the picker lists the whole "
                           "top 300.")
            k1, k2, k3 = st.columns([2, 2, 2])
            with k1:
                # the filter can remove the selected account: set the replacement explicitly so the dropdown
                # shows it too (deleting the key let the graph move to the first account while the box kept the old one)
                if st.session_state.get(picker_key) not in options:
                    st.session_state[picker_key] = options[0]
                acct = st.selectbox("Account", options, key=picker_key, on_change=clear_tick)
            with k2:
                labels_on = st.segmented_control(
                    "Labels on the graph", ["Off", "Flagged accounts", "All accounts"], default="Flagged accounts",
                    help="Writes a short remark next to each dot. 'All accounts' is easiest to read full screen.")
            with k3:
                st.write("")
                show_truth = st.toggle("Reveal IBM's answer key", value=False,
                                       help="Laundering transfers turn red. The labels are only used to check the "
                                            "watchlist afterwards; they are not used to draw the graph or to score "
                                            "any account.")
            edges = load("q2_watchlist_edges", variant)
            if not edges.empty and acct:
                sub = edges[(edges.src_id == acct) | (edges.dst_id == acct)]
                nbrs = set(sub.src_id) | set(sub.dst_id)
                sub = edges[edges.src_id.isin(nbrs) & edges.dst_id.isin(nbrs)]
                g = nx.DiGraph()
                for r in sub.itertuples():
                    g.add_edge(r.src_id, r.dst_id, usd=r.usd, n=r.n_txn, bad=r.any_laundering)
                g.add_node(acct)
                risk = dict(zip(wl.acct, wl.risk_probability))
                rank = dict(zip(wl.acct, wl.risk_rank))
                # rings, not a force layout: a force layout pulls a laundering ring into a dense knot where labels
                # collide. Here every flagged counterparty gets its own slot on the inner ring.
                inner = sorted((n for n in g.nodes if n in risk and n != acct), key=lambda n: rank[n])
                outer = sorted((n for n in g.nodes if n not in risk),
                               key=lambda n: -sum(g.edges[e]["usd"] for e in ((acct, n), (n, acct)) if g.has_edge(*e)))
                pos = {acct: (0.0, 0.0)}

                def ring(members, radius, start):
                    for i, n in enumerate(members):
                        a = start + 2 * np.pi * i / len(members)
                        pos[n] = (radius * np.cos(a), radius * np.sin(a))

                ring(inner, 0.6 if outer else 0.8, np.pi / 2)
                if inner and outer:
                    # outer accounts go in the gaps between inner accounts, with a clear margin around each inner
                    # account's direction, so arrows to the outer ring never run through an inner label
                    step = 2 * np.pi / len(inner)
                    margin = min(np.radians(14), step / 4)
                    gaps = [outer[gi::len(inner)] for gi in range(len(inner))]   # deal round-robin: big flows spread out
                    for gi, members in enumerate(gaps):
                        lo = np.pi / 2 + gi * step + margin
                        hi = np.pi / 2 + (gi + 1) * step - margin
                        for j, n in enumerate(members):
                            a = lo + (j + 1) * (hi - lo) / (len(members) + 1)
                            pos[n] = (np.cos(a), np.sin(a))
                else:
                    ring(outer, 1.0, np.pi / 2)
                rows = {r.acct: r for r in wl.itertuples()}
                me = f"#{int(rank[acct]):,}" if acct in rank else "this account"
                nodes = list(g.nodes)
                flagged = [n for n in nodes if n in risk]
                unflagged = [n for n in nodes if n not in risk]

                def flows(n, short=False):
                    """Money between the selected account and n, from n's point of view."""
                    got = g.edges[acct, n] if g.has_edge(acct, n) else None
                    paid = g.edges[n, acct] if g.has_edge(n, acct) else None
                    parts = ([f"received {money(got['usd'])}" + ("" if short else f" from {me}")] if got else []) + \
                            ([f"paid {money(paid['usd'])}" + ("" if short else f" to {me}")] if paid else [])
                    return ", ".join(parts)

                # arrows: payer -> payee, coloured by direction relative to the selected account;
                # transfers between its counterparties stay faint so the selected account's flows stand out
                IN_C, OUT_C, OTHER_C, BAD_C = T["s1"], T["s2"], rgba(T["muted"], 0.30), T["bad"]
                arrows, mx, my, mtext = [], [], [], []
                for u, v, d in g.edges(data=True):
                    red = bool(show_truth and d["bad"])
                    mine = acct in (u, v)
                    colour = BAD_C if red else IN_C if v == acct else OUT_C if u == acct else OTHER_C
                    arrows.append(dict(x=pos[v][0], y=pos[v][1], ax=pos[u][0], ay=pos[u][1],
                                       xref="x", yref="y", axref="x", ayref="y", text="", showarrow=True,
                                       arrowhead=2, arrowsize=1.1 if (red or mine) else 0.7,
                                       arrowwidth=2.5 if red else 1.6 if mine else 0.8, arrowcolor=colour,
                                       standoff=14 if v == acct else 7, startstandoff=14 if u == acct else 7))
                    mx.append((pos[u][0] + pos[v][0]) / 2)
                    my.append((pos[u][1] + pos[v][1]) / 2)
                    mtext.append(f"{u} → {v}<br>{int(d['n']):,} transfer(s), {money(d['usd'])}"
                                 + ("<br><b>laundering (answer key)</b>" if red else ""))

                # smart label placement: each label points away from the centre of its own group,
                # so labels fan outwards instead of piling up inside the cluster
                def centre(group):
                    pts = [pos[n] for n in group] or [pos[acct]]
                    return (sum(p[0] for p in pts) / len(pts), sum(p[1] for p in pts) / len(pts))

                all_c = centre([n for n in nodes if n != acct])
                SIDES = ["middle right", "top right", "top center", "top left",
                         "middle left", "bottom left", "bottom center", "bottom right"]

                def side(n):
                    cx, cy = all_c if n == acct else pos[acct]          # rings: every label points outwards
                    dx, dy = pos[n][0] - cx, pos[n][1] - cy
                    if abs(dx) < 1e-9 and abs(dy) < 1e-9:
                        return "top center"
                    return SIDES[int(((np.degrees(np.arctan2(dy, dx)) + 22.5) % 360) // 45)]

                def label(n):
                    if labels_on not in ("Flagged accounts", "All accounts"):
                        return ""
                    if n == acct:
                        return ""                                # drawn as a chip (annotation) instead
                    if n in risk:
                        # rank, red flags, then the money it moved with the selected account (if any direct flow)
                        money_line = flows(n, short=True)
                        return (f"<b>#{int(rank[n]):,}</b><br>{flag_tags(rows[n], limit=2) or 'channel risk'}"
                                + (f"<br>{money_line}" if money_line else ""))
                    return flows(n, short=True) if labels_on == "All accounts" else ""

                def hover(n):
                    if n in risk:
                        extra = "" if n == acct else f"<br>{flows(n)}"
                        return (f"<b>{n}</b><br>watchlist #{int(rank[n]):,}, risk {risk[n]:.2f}"
                                f"<br>{flag_tags(rows[n], limit=9) or 'channel risk only'}{extra}")
                    return f"<b>{n}</b><br>not on the watchlist<br>{flows(n)}"

                # axis ranges first: label placement works in pixels, so it needs the same data -> pixel mapping
                xs, ys = [p[0] for p in pos.values()], [p[1] for p in pos.values()]
                padx, pady = (max(xs) - min(xs)) * 0.18 + 0.05, (max(ys) - min(ys)) * 0.10 + 0.05
                xr, yr = (min(xs) - padx, max(xs) + padx), (min(ys) - pady, max(ys) + pady)
                # plot area in px: 600 high minus margins; width = the graph column (3/5 of the wide page) minus
                # margins and colour bar. Assumed on the narrow side, so labels that fit here fit wider screens too.
                PW, PH = 640, 520
                to_px = lambda x, y: ((x - xr[0]) / (xr[1] - xr[0]) * PW, (yr[1] - y) / (yr[1] - yr[0]) * PH)
                radius = {n: (16 if n == acct else 8.5 if n in risk else 7) for n in nodes}
                obstacles = [(px - radius[n], py - radius[n], px + radius[n], py + radius[n])
                             for n in nodes for px, py in [to_px(*pos[n])]]
                if labels_on in ("Flagged accounts", "All accounts"):        # the "#1 selected" chip above the centre
                    cx, cy = to_px(*pos[acct])
                    chip_w = (len(me) + 9) * 13 * 0.6 + 10
                    obstacles.append((cx - chip_w / 2, cy - 26 - 13, cx + chip_w / 2, cy - 26 + 13))
                items = [(n, pos[n], radius[n], 12, side(n), [label(n), f"<b>#{int(rank[n]):,}</b>"])
                         for n in inner if label(n)] +                         [(n, pos[n], radius[n], 11, side(n), [label(n)]) for n in outer if label(n)]
                # arrows as pixel segments: the selected account's own flows weigh more, so labels sit over the
                # faint between-counterparty lines before they sit over a bold arrow
                segments = [(3 if acct in (u, v) else 1, (to_px(*pos[u]), to_px(*pos[v]))) for u, v in g.edges]
                placed = place_labels(items, obstacles, to_px, segments=segments)
                hidden = sum(1 for n, _, _, _, _, t in items if placed[n][1] != t[0])
                lab = lambda n: placed[n][1] if n in placed else ""
                where = lambda n: placed[n][0] if n in placed else side(n)

                key = [(IN_C, f"Money into {me}"), (OUT_C, f"Money out of {me}"),
                       (rgba(T["muted"], 0.6), "Between its counterparties")] + \
                      ([(BAD_C, "Laundering (answer key)")] if show_truth else [])
                fig = go.Figure(
                    [go.Scatter(x=[None], y=[None], mode="lines", line=dict(color=c, width=3), name=n,
                                hoverinfo="skip") for c, n in key]
                    + [go.Scatter(x=[None], y=[None], mode="markers", name="Not flagged", hoverinfo="skip",
                                  marker=dict(size=10, color=T["unflagged"]))]
                    + [go.Scatter(x=mx, y=my, mode="markers", marker=dict(size=14, color="rgba(0,0,0,0)"),
                                  text=mtext, hoverinfo="text", showlegend=False)]
                    + [go.Scatter(x=[pos[n][0] for n in unflagged], y=[pos[n][1] for n in unflagged],
                                  customdata=unflagged, mode="markers", showlegend=False,
                                  marker=dict(size=10, color=T["unflagged"], line=dict(width=2, color=T["surface"])),
                                  hovertext=[hover(n) for n in unflagged], hoverinfo="text")]
                    + [go.Scatter(x=[pos[n][0] for n in flagged], y=[pos[n][1] for n in flagged],
                                  customdata=flagged, mode="markers", showlegend=False,
                                  marker=dict(size=[26 if n == acct else 13 for n in flagged],
                                              color=[risk[n] for n in flagged], colorscale=T["risk"], cmin=0, cmax=1,
                                              line=dict(width=[2.5 if n == acct else 2 for n in flagged],
                                                        color=[T["ink"] if n == acct else T["surface"] for n in flagged]),
                                              colorbar=dict(title=dict(text="risk score",
                                                                       font=dict(color=T["muted"], size=12)),
                                                            tickfont=dict(color=T["ink2"]), outlinewidth=0,
                                                            thickness=10, len=0.6, y=0.4)),
                                  hovertext=[hover(n) for n in flagged], hoverinfo="text")])
                label_bg = rgba(T["surface"], 0.85)
                arrows += [label_annotation(*pos[n], where(n), lab(n), radius[n], 12 if n in risk else 11,
                                            T["ink"] if n in risk else T["ink2"], label_bg)
                           for n in nodes if lab(n)]
                if labels_on in ("Flagged accounts", "All accounts"):
                    arrows.append(dict(x=pos[acct][0], y=pos[acct][1], xref="x", yref="y", showarrow=False, yshift=26,
                                       text=f"<b>{me}</b> selected", font=dict(size=13, color=T["ink"]),
                                       bgcolor=rgba(T["surface"], 0.9), bordercolor=T["axis"], borderpad=4))
                fig.update_layout(annotations=arrows, clickmode="event+select")
                fig.update_xaxes(visible=False, range=list(xr))
                fig.update_yaxes(visible=False, range=list(yr))
                fig = style(fig, height=600)
                fig.update_layout(margin=dict(l=40, r=40, t=50, b=30))

                gcol, pcol = st.columns([3, 2])
                gcol.plotly_chart(fig, width="stretch", theme=None, key=chart_key,
                                  on_select=follow_trail_node, selection_mode="points")
                gcol.caption("Click a top-300 account in the filtered list to open its trail. Arrows point from payer to "
                             "payee: transfers on the learning days, with the account's 40 largest counterparties by USD. "
                             "Hover a dot or the middle of an arrow for details; use the full-screen button for the "
                             "clearest view."
                             + (f" {hidden} label{'s' if hidden != 1 else ''} hidden or shortened so none overlap; "
                                "hover those dots for details." if hidden else ""))

                # investigator panel: who belongs in one case, and where money left the flagged group
                with pcol:
                    inside = [d for u, v, d in g.edges(data=True) if u in risk and v in risk]
                    exits = sorted(((d["usd"], v) for u, v, d in g.edges(data=True) if u in risk and v not in risk),
                                   reverse=True)
                    if len(flagged) > 1:
                        others = sorted(int(rank[n]) for n in flagged if n != acct)
                        listed = ", ".join(f"#{r:,}" for r in others[:8]) + (" and more" if len(others) > 8 else "")
                        st.info(f"**Suggested case:** {me} with {len(others)} flagged counterpart"
                                f"{'y' if len(others) == 1 else 'ies'} ({listed}). Between them they moved "
                                f"{money_md(sum(d['usd'] for d in inside))} in "
                                f"{sum(int(d['n']) for d in inside):,} transfers"
                                + (f". Largest flow out to an unflagged account: {money_md(exits[0][0])} to "
                                   f"`{exits[0][1]}`, worth checking as a possible cash-out point." if exits else "."))
                    row = wl[wl.acct == acct].iloc[0]
                    why = [r.strip() for r in str(row.reasons if pd.notna(row.reasons) else "").split(";")
                           if r.strip()]
                    bullets = "\n".join(f"- {r}" for r in why) if why else "- Channel risk only"
                    st.markdown(f"**Why {me} was flagged**\n\n{bullets}")
                    if has_split:
                        st.markdown("**Account half:** " + (
                            "graded. Its own labels were never used to build the scorecard."
                            if row["split"] == "holdout" else
                            "learning. Its labels helped set the points per red flag; its score still comes only "
                            "from its own money-flow patterns."))
                    table = pd.DataFrame([{
                        "account": n,
                        "rank": int(rank[n]) if n in rank else None,
                        "risk score": round(risk[n] * 100, 1) if n in risk else None,
                        "red flags": (flag_tags(rows[n], limit=9) or "channel risk") if n in risk else "not flagged",
                        f"money with {me}": "selected account" if n == acct else flows(n, short=True),
                        "_usd": sum(g.edges[e]["usd"] for e in ((acct, n), (n, acct)) if g.has_edge(*e)),
                    } for n in nodes])
                    table = pd.concat([table[table.account == acct],
                                       table[(table.account != acct) & table["rank"].notna()].sort_values("rank"),
                                       table[table["rank"].isna()].sort_values("_usd", ascending=False)])
                    st.markdown(f"**Accounts in this money trail** ({len(nodes)})")
                    st.dataframe(table.drop(columns="_usd"), hide_index=True, width="stretch", height=300,
                                 column_config={"rank": st.column_config.NumberColumn("rank", format="#%d")})

        st.subheader("How the watchlist is scored and graded")
        st.caption("Covers all accounts. Nothing here changes with the search, filter or selected account above.")
        # OFFICIAL figure first (docs/decisions.md): accounts are scored from the learning days only and graded on
        # whether they launder on the LATER days; the tail days (almost all laundering) are shown separately.
        if not official.empty:
            off = official
            tail = evt[evt.period == "incl_tail"].set_index("k")
            show(precision_chart(off, series, "<b>Official grade</b>: flagged from the learning days, graded on the "
                                              "following days (graded half of the accounts only)",
                                 "launder on the later days"))
            if 1000 in tail.index and (off.k == 1000).any():
                o1k = off.set_index("k").loc[1000]
                st.caption(f"Top 1,000: {pct(o1k.precision_combined)} on the grading days vs {pct(o1k.base_rate, 2)} "
                           f"at random. Including the tail days (almost all laundering): "
                           f"{pct(tail.loc[1000, 'precision_combined'])}; we report the first figure.")
            with st.expander("Show the numbers"):
                st.dataframe(off.assign(**{n: (off[c] * 100).round(1) for c, n in series})[
                    ["k"] + [n for _, n in series]].rename(columns={"k": "watchlist size"}),
                    hide_index=True, width="stretch")
        if not ev.empty:
            e = ev.sort_values("k")
            with st.expander("Supporting test: same days (schemes already under way)", expanded=official.empty):
                show(precision_chart(e, series, "<b>Same days</b>: precision of the top-k watchlist on the graded "
                                                "half (labels never used)", "truly involved in laundering"))
                st.caption("Spotting schemes that are already running is easier than predicting who launders next, "
                           "so this figure is higher than the official one above.")
        sp = load("q2_signal_precision", variant)
        pc = load("q2_pattern_coverage", variant)
        c1, c2 = st.columns(2)
        if not sp.empty:
            c1.subheader("What each red flag is worth (weight of evidence)")
            flags = sp[sp.value > 0].sort_values("woe", ascending=False).copy()
            flags["red flag"] = [SIGNAL_NAMES.get(s, s) + (f" (level {int(v)})" if s == "segment" else "")
                                 for s, v in zip(flags.signal, flags.value)]
            flags["hit rate"] = (flags.precision_calibration * 100).round(1)
            c1.dataframe(flags[["red flag", "woe", "n_cal", "n_cal_bad", "hit rate"]].round(3),
                         hide_index=True, width="stretch",
                         column_config={"red flag": st.column_config.TextColumn("red flag", width="medium"),
                                        "woe": st.column_config.NumberColumn("points", format="%.2f", width="small",
                                                                             help="Weight of evidence."),
                                        "n_cal": st.column_config.NumberColumn("flagged", format="%d", width="small"),
                                        "n_cal_bad": st.column_config.NumberColumn("launderers", format="%d",
                                                                                   width="small"),
                                        "hit rate": st.column_config.NumberColumn(
                                            "hit rate", format="%.1f%%", width="small",
                                            help="Share of the flagged accounts that were launderers.")})
            c1.caption("Learned on the learning half of the accounts, learning days only. More points = stronger "
                       "evidence; an account's score adds up the points of every flag it shows.")
        if not pc.empty:
            c2.subheader("Laundering typologies caught by the watchlist")
            show(hbar(pc.pattern_type.str.replace("-", " ").str.title(), pc.detection_rate * 100,
                      lambda v: f"{v:.0f}%", "schemes touched by the top-1,000 watchlist", x_max=112).update_xaxes(
                tickvals=[0, 20, 40, 60, 80, 100], ticksuffix="%"), c2)
            c2.caption(f"{int(pc.n_detected.sum()):,} of {int(pc.n_attempts.sum()):,} known schemes "
                       f"({pct(pc.n_detected.sum() / max(pc.n_attempts.sum(), 1), 0)}) touch at least one watchlist "
                       "account, across every shape, including shapes we have no detector for (account reuse). "
                       "Checked against IBM's answer key over all days and accounts: coverage, not the official grade.")

# ----------------------------------------------------------------------------------------------- live
with tab_live:
    st.header("Live monitor: batch insights applied to every new transaction")
    st.caption("Kafka topic `aml-transactions` (3 partitions, keyed by sender) → consumer applies the BQ2 watchlist, "
               "BQ1 hot segments and a 24-hour velocity window → alerts land here within milliseconds.")
    if st.button("Refresh"):
        s3_alerts.clear()
        st.rerun()
    alerts, stats = load_alerts(variant)
    base_txt = pct(dq.iloc[0].laundering_rows / max(dq.iloc[0].rows_out, 1), 2) if not dq.empty else "about 0.1%"
    if alerts.empty:
        st.info("No alerts yet - start streaming/producer.py and streaming/consumer.py on the Kafka EC2 instance.")
    else:
        c = st.columns(5)
        c[0].metric("Events processed", f"{stats.get('events', len(alerts)):,}")
        c[1].metric("Alerts raised", f"{stats.get('alerts', len(alerts)):,}")
        c[2].metric("Alert rate", pct(stats.get("alert_rate", 0), 2), help="Share of streamed transfers that raised an alert.")
        c[3].metric("Alert precision", pct(stats.get("alert_precision", alerts.is_laundering.mean()), 2),
                    help="Share of alerts that are truly laundering (IBM label, used only for this figure). "
                         f"Overall {base_txt} of transfers are laundering, which is what random alerts would score.")
        lat50, lat95 = stats.get("latency_ms_p50"), stats.get("latency_ms_p95")
        c[4].metric("Detection latency p50", f"{lat50} ms" if lat50 is not None else "-",
                    f"p95 {lat95} ms" if lat95 is not None else None, delta_color="off",
                    help="Time from the producer sending a transfer to the consumer scoring it.")
        a = alerts.copy()
        a["rule"] = a.reasons.str.extract(r"^(sender on network|receiver on network|high-risk segment|velocity)")[0]
        c1, c2 = st.columns([2, 1])
        c1.dataframe(a.sort_values("alerted_at", ascending=False).head(200)[
            ["ts", "src_id", "dst_id", "amount_usd", "payment_format", "severity", "reasons"]],
            hide_index=True, width="stretch", height=400,
            column_config={"ts": "time", "src_id": "from account", "dst_id": "to account",
                           "amount_usd": st.column_config.NumberColumn("amount (USD)", format="%.0f"),
                           "payment_format": "payment",
                           "severity": st.column_config.ProgressColumn("severity", format="%.2f",
                                                                       min_value=0, max_value=1),
                           "reasons": "why it alerted"})
        counts = a.rule.fillna("other").str.capitalize().value_counts()
        c2.markdown("**Alerts by rule**")
        show(hbar(counts.index, counts.values, lambda v: f"{int(v):,}", "alerts", height=400), c2)

# ----------------------------------------------------------------------------------------------- ops
with tab_ops:
    st.header("Pipeline & cost evidence")
    st.subheader("How the pipeline works")
    st.image(architecture_svg(ui_mode()), width="stretch")
    st.caption("Solid red: the batch trail, run daily in one click by Step Functions. Dashed: the live trail, where "
               "Kafka applies the batch results to each new transfer. Icons: AWS Architecture Icons, Simple Icons.")
    st.subheader("Why each service")
    st.markdown("""
| Stage | AWS service | Why this choice (lecture reference) |
|---|---|---|
| Ingestion | EC2 + Kaggle API → **S3 raw zone** | data lake, schema-on-read (W4 slides 30-33) |
| Preparation | **Glue ETL** (Spark) → Parquet, partitioned by date | columnar + partitioned = less data scanned (W2 slides 70-73) |
| Catalog | **Glue crawler + Data Catalog** | metadata for Athena (W4 Glue lab) |
| BQ1 analytics | **Athena** (serverless SQL, UNLOAD to Parquet) | pay per byte scanned (W5 slide 36) |
| BQ2 analytics | **EMR** transient Spark cluster | in-memory multi-hop joins (W6 slides 28, 33) |
| Orchestration | **Step Functions** | ordered, retried, auto-terminating (W4 slide 38) |
| Speed layer | **Kafka** on EC2 | event backbone, partitions + consumer groups (W7) |
| Serving | **Streamlit** + boto3 → Athena | small aggregated results only (W5 slide 37) |
""")
    if MODE == "athena" and BUCKET:
        df = run_logs()
        if not df.empty:
            if {"data_scanned_mb", "cost_usd"} <= set(df.columns):
                c = st.columns(3)
                c[0].metric("Athena queries run by the pipeline", f"{len(df):,}")
                c[1].metric("Data scanned in total", f"{df.data_scanned_mb.sum() / 1024:,.2f} GB")
                c[2].metric("Athena cost in total", f"${df.cost_usd.sum():,.2f}",
                            help="Athena bills per byte scanned (minimum 10 MB per query).")
            bench = df[df.label.str.startswith("benchmark:") & ~df.label.str.contains("first_day")].copy()
            if not bench.empty:
                bench["test"] = bench.label.str.split(":").str[1]
                summ = bench.groupby(["variant", "test"])[["data_scanned_mb", "engine_seconds", "cost_usd"]].median().reset_index()
                st.subheader("CSV vs. partitioned Parquet - same question, measured (median of 3 runs)")
                pairs = []
                for (v, t), r in summ.set_index(["variant", "test"]).iterrows():
                    if t.endswith("csv") and (v, t[:-3] + "parquet") in summ.set_index(["variant", "test"]).index:
                        pq = summ.set_index(["variant", "test"]).loc[(v, t[:-3] + "parquet")]
                        pairs.append((v, t[:-4].replace("_", " "), r.data_scanned_mb / max(pq.data_scanned_mb, 1e-9)))
                if pairs:
                    cols_ = st.columns(len(pairs))
                    for col, (v, t, x) in zip(cols_, pairs):
                        col.metric(f"{v}, {t}: Parquet scans", f"{x:,.0f}x less data",
                                   help="Same question asked of the raw CSV and of the partitioned Parquet; "
                                        "Athena bills by data scanned.")
                st.dataframe(summ, hide_index=True, width="stretch",
                             column_config={"data_scanned_mb": st.column_config.NumberColumn("MB scanned",
                                                                                             format="%.1f"),
                                            "engine_seconds": st.column_config.NumberColumn("seconds", format="%.2f"),
                                            "cost_usd": st.column_config.NumberColumn("cost (USD)", format="%.5f")})
                vcol = {v: c for v, c in zip(sorted(summ.variant.unique()), [T["s1"], T["s2"], T["s3"]])}
                fig = go.Figure([go.Bar(x=d.test, y=d.data_scanned_mb, name=v, marker=dict(color=vcol.get(v, T["s1"])),
                                        text=[f"{x:,.1f} MB" for x in d.data_scanned_mb], textposition="outside",
                                        cliponaxis=False, constraintext="none", textfont=dict(color=T["ink"], size=12),
                                        hovertemplate=f"{v}, %{{x}}: %{{y:,.1f}} MB<extra></extra>")
                                 for v, d in summ.groupby("variant")])
                fig.update_layout(barmode="group", bargap=0.55, bargroupgap=0.15,
                                  title=dict(text="Data scanned per query (log scale)", x=0, xanchor="left", y=0.97))
                fig.update_yaxes(type="log", title="MB scanned", dtick=1, tickformat=",")
                fig = style(fig, height=440)
                fig.update_layout(margin=dict(t=90), legend=dict(y=1.0))
                show(fig)
            st.subheader("All Athena queries run by the pipeline")
            st.dataframe(df.sort_values("at", ascending=False), hide_index=True, width="stretch",
                         column_config={"data_scanned_mb": st.column_config.NumberColumn("MB scanned", format="%.1f"),
                                        "engine_seconds": st.column_config.NumberColumn("seconds", format="%.2f"),
                                        "cost_usd": st.column_config.NumberColumn("cost (USD)", format="%.5f"),
                                        "at": "run at"})
    else:
        st.info("Cost/performance logs appear here in athena mode (s3://BUCKET/analytics/run_log/).")
