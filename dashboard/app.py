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
import time

import networkx as nx
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

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


SIGNAL_NAMES = {"sg_end": "Scatter-gather: start or end of the split", "sg_mule": "Scatter-gather: mule in the middle",
                "cycle": "Money cycle (2-4 hops back to sender)", "fan_out": "Fan-out (pays many in one day)",
                "gather_scatter": "Gather-scatter (collects and pays many, same day)",
                "fan_in": "Fan-in (paid by many in one day)", "segment": "Uses high-risk channels (Q1)"}


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
st.sidebar.markdown("---\n**Pipeline:** S3 → Glue ETL → Glue Catalog → Athena (Q1) → EMR Spark (Q2) → "
                    "Kafka live monitor → this dashboard. Orchestrated by Step Functions.")

dq = load("dq_report", variant)
tab_overview, tab_q1, tab_q2, tab_live, tab_ops = st.tabs(
    ["Overview", "Q1: Risk hotspots", "Q2: Laundering networks", "Live monitor", "Pipeline & cost"])

# ----------------------------------------------------------------------------------------------- overview
with tab_overview:
    st.header("Where is the dirty money, and who is moving it?")
    st.write("Two questions, one pipeline: **Q1** tells the monitoring team *which channels* to watch; "
             "**Q2** tells investigators *which accounts* form laundering networks, and why.")
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
    # Headline Q2 figure = the OFFICIAL time-based grade (docs/decisions.md); same-days only as a fallback
    official = (evt[evt.period == "official"].sort_values("k")
                if not evt.empty and (evt.n_holdout_bad > 0).any() else pd.DataFrame())
    if not seg.empty and (not official.empty or not ev.empty):
        e = official if not official.empty else ev.sort_values("k")
        k100 = e[e.k == 100].iloc[0] if (e.k == 100).any() else e.iloc[0]
        cap = seg.sort_values("risk_rank")
        cap5 = cap[cap.cum_txn_share <= 0.05].cum_laundering_share.max()
        c = st.columns(3)
        c[0].metric("Laundering caught by reviewing the riskiest 5% of transactions (Q1)", pct(cap5 if pd.notna(cap5) else 0),
                    help="Channel risk is learned on the learning days and measured on those same days.")
        if not official.empty:
            c[1].metric("Top-100 watchlist: share that launders on the later days (Q2, official)",
                        pct(k100.precision_combined),
                        f"{k100.precision_combined / max(k100.base_rate, 1e-9):.0f}x better than random",
                        delta_color="normal",
                        help="Flagged using only the learning days, graded on the following days, on the half of the "
                             "accounts whose labels were never used to build the scorecard.")
            c[2].metric("Graded accounts that launder on the later days",
                        f"{int(k100.n_holdout_bad):,} of {int(k100.n_holdout_accounts):,}",
                        f"{pct(k100.base_rate)} = picking at random", delta_color="off")
        else:
            c[1].metric("Precision of top-100 watchlist (Q2, same days, unseen accounts)", pct(k100.precision_combined),
                        f"{k100.precision_combined / max(k100.base_rate, 1e-9):.0f}x better than random", delta_color="normal")
            c[2].metric("Laundering accounts in holdout set", f"{int(k100.n_holdout_bad):,} of {int(k100.n_holdout_accounts):,}")

# ----------------------------------------------------------------------------------------------- Q1
with tab_q1:
    st.header("Q1: Where should the monitoring team spend its limited review capacity?")
    if seg.empty:
        st.info("Run the pipeline to populate Q1 results.")
    else:
        seg = seg.sort_values("risk_rank")
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
        fig.add_trace(go.Scatter(x=curve.cum_txn_share * 100, y=curve.cum_laundering_share * 100,
                                 mode="lines", name="Risk-ranked monitoring", line=dict(width=3)))
        fig.add_trace(go.Scatter(x=[0, 100], y=[0, 100], mode="lines", name="Random review",
                                 line=dict(dash="dash", color="grey")))
        fig.add_vline(x=budget * 100, line_dash="dot", annotation_text=f"budget {budget * 100:.1f}%")
        fig.update_layout(xaxis_title="% of transactions reviewed", yaxis_title="% of laundering caught",
                          height=380, margin=dict(t=20), xaxis_range=[0, 30])
        st.plotly_chart(fig, width="stretch")
        st.markdown("**Monitoring rules to deploy at this budget** (ranked by the 95% Wilson lower bound of the "
                    "laundering rate, so small lucky segments cannot top the list):")
        show = chosen.head(25).assign(
            corridor=lambda d: d.is_cross_bank.map({1: "cross-bank", 0: "same bank"}),
            fx=lambda d: d.is_cross_currency.map({1: "cross-currency", 0: "same currency"}),
            rate=lambda d: (d.laundering_rate * 100).round(2), lift=lambda d: d.lift.round(1))
        st.dataframe(show[["risk_rank", "payment_format", "payment_currency", "corridor", "fx", "amount_band",
                           "n_txn", "n_laundering", "rate", "lift"]], hide_index=True, width="stretch")

        fc = load("q1_format_currency", variant)
        tp = load("q1_time_profile", variant)
        c1, c2 = st.columns(2)
        if not fc.empty:
            h = fc[fc.n_txn >= 30].pivot_table(index="payment_currency", columns="payment_format", values="lift")
            c1.subheader("Risk lift: payment format × currency")
            c1.plotly_chart(px.imshow(h.round(1), text_auto=True, aspect="auto", color_continuous_scale="Reds",
                                      labels=dict(color="lift")), width="stretch")
        if not tp.empty:
            t = tp.assign(day=tp.day_of_week.map(DOW)).pivot_table(index="day", columns="txn_hour", values="lift")
            t = t.reindex([d for d in DOW.values() if d in t.index])
            c2.subheader("When does laundering happen? (lift by hour)")
            c2.plotly_chart(px.imshow(t.round(1), aspect="auto", color_continuous_scale="Reds",
                                      labels=dict(color="lift", x="hour")), width="stretch")
        bc = load("q1_bank_corridors", variant)
        if not bc.empty:
            st.subheader("Bank-to-bank corridors carrying the most laundered value")
            top = bc.sort_values("laundered_usd", ascending=False).head(15)
            st.dataframe(top.assign(laundered_usd=top.laundered_usd.round(0), lift=top.lift.round(1)),
                         hide_index=True, width="stretch")

# ----------------------------------------------------------------------------------------------- Q2
with tab_q2:
    st.header("Q2: Which accounts form laundering networks, and why?")
    wl = load("q2_watchlist", variant)
    if wl.empty:
        st.info("Run the pipeline to populate Q2 results.")
    else:
        wl = wl.sort_values("risk_rank")
        series = [("precision_combined", "Combined scorecard (Q1 + Q2)"),
                  ("precision_network_only", "Network motifs only"),
                  ("precision_segment_only", "Channel rules only (Q1)"),
                  ("base_rate", "Random selection")]
        # OFFICIAL figure first (docs/decisions.md): accounts are scored from the learning days only and graded on
        # whether they launder on the LATER days; the tail days (almost all laundering) are shown separately.
        if not official.empty:
            off = official
            tail = evt[evt.period == "incl_tail"].set_index("k")
            fig2 = go.Figure()
            for col, name in series:
                fig2.add_trace(go.Bar(x=off.k.astype(str), y=off[col] * 100, name=name,
                                      text=(off[col] * 100).round(1).astype(str) + "%", textposition="outside"))
            fig2.update_layout(barmode="group", height=360, margin=dict(t=40),
                               title="OFFICIAL: flagged from the learning days, graded on the following days "
                                     "(graded half of the accounts only)",
                               xaxis_title="watchlist size k", yaxis_title="% laundering on the later days")
            st.plotly_chart(fig2, width="stretch")
            if 1000 in tail.index and (off.k == 1000).any():
                o1k = off.set_index("k").loc[1000]
                st.caption(f"Top 1,000: {pct(o1k.precision_combined)} on the grading days vs {pct(o1k.base_rate, 2)} "
                           f"at random. Including the tail days (almost all laundering): "
                           f"{pct(tail.loc[1000, 'precision_combined'])} — we report the first figure.")
        if not ev.empty:
            e = ev.sort_values("k")
            with st.expander("Supporting test: same days (schemes already under way)", expanded=official.empty):
                fig = go.Figure()
                for col, name in series:
                    fig.add_trace(go.Bar(x=e.k.astype(str), y=e[col] * 100, name=name))
                fig.update_layout(barmode="group", height=330, margin=dict(t=30),
                                  title="Same days: precision of top-k watchlist on the graded half (labels never used)",
                                  xaxis_title="watchlist size k", yaxis_title="% truly involved in laundering")
                st.plotly_chart(fig, width="stretch")
                st.caption("Spotting schemes that are already running is easier than predicting who launders next, "
                           "so this figure is higher than the official one above.")
        HALF = {"holdout": "graded (labels never used)", "calibration": "learning (labels set the points)"}
        has_split = "split" in wl.columns
        c1, c2 = st.columns([3, 2])
        with c1:
            st.subheader("Investigator watchlist")
            q = st.text_input("Filter by account id or reason (e.g. 'cycle', 'mule')", "")
            view = wl[wl.acct.str.contains(q, case=False, regex=False)
                      | wl.reasons.fillna("").str.contains(q, case=False, regex=False)] if q else wl
            if has_split and st.toggle("Only the graded half (accounts whose labels were never used)", value=False):
                view = view[view["split"] == "holdout"]
            cols = ["risk_rank", "acct", "risk"] + (["half"] if has_split else []) + ["reasons"]
            st.dataframe(view.head(300).assign(risk=lambda d: (d.risk_probability * 100).round(1),
                                               half=lambda d: d["split"].map(HALF) if has_split else "")[cols],
                         hide_index=True, width="stretch", height=420,
                         column_config={"risk": st.column_config.NumberColumn("risk score", format="%.1f"),
                                        "half": st.column_config.TextColumn("account half")})
            st.caption(f"{len(view):,} of {len(wl):,} watchlist accounts match. Scores use only each account's own "
                       "money-flow patterns; the points per red flag were learned from the labels of the learning "
                       "half, on the learning days.")
        with c2:
            st.subheader("Money-trail explorer")
            # money trails are published for the top 300 accounts; the picker follows the watchlist filter
            options = view[view.risk_rank <= 300].acct.tolist() or wl.acct.head(300).tolist()
            acct = st.selectbox("Account", options)
            edges = load("q2_watchlist_edges", variant)
            if not edges.empty and acct:
                sub = edges[(edges.src_id == acct) | (edges.dst_id == acct)]
                nbrs = set(sub.src_id) | set(sub.dst_id)
                sub = edges[edges.src_id.isin(nbrs) & edges.dst_id.isin(nbrs)]
                g = nx.DiGraph()
                for r in sub.itertuples():
                    g.add_edge(r.src_id, r.dst_id, usd=r.usd, n=r.n_txn, bad=r.any_laundering)
                pos = nx.spring_layout(g, seed=7, k=0.9)
                show_truth = st.toggle("Reveal IBM's answer key (laundering transfers in red)", value=False,
                                       help="The labels are only used to check the watchlist afterwards; "
                                            "they are not used to draw the graph or to score this account.")
                risk = dict(zip(wl.acct, wl.risk_probability))
                rank = dict(zip(wl.acct, wl.risk_rank))
                # arrows: one annotation per transfer pair, pointing from payer to payee
                arrows, mx, my, mtext = [], [], [], []
                for u, v, d in g.edges(data=True):
                    red = bool(show_truth and d["bad"])
                    arrows.append(dict(x=pos[v][0], y=pos[v][1], ax=pos[u][0], ay=pos[u][1],
                                       xref="x", yref="y", axref="x", ayref="y", text="", showarrow=True,
                                       arrowhead=2, arrowsize=1.2 if red else 1, arrowwidth=2.5 if red else 1,
                                       arrowcolor="crimson" if red else "#999",
                                       standoff=12 if v == acct else 7, startstandoff=12 if u == acct else 7))
                    mx.append((pos[u][0] + pos[v][0]) / 2)
                    my.append((pos[u][1] + pos[v][1]) / 2)
                    mtext.append(f"{u} → {v}<br>{int(d['n']):,} transfer(s), ${d['usd']:,.0f}"
                                 + ("<br><b>laundering (answer key)</b>" if red else ""))

                def label(n):
                    if n in risk:
                        return f"{n}<br>watchlist #{int(rank[n]):,}, risk {risk[n]:.2f}"
                    return f"{n}<br>not on watchlist"

                fig = go.Figure([
                    go.Scatter(x=mx, y=my, mode="markers", marker=dict(size=9, color="rgba(0,0,0,0)"),
                               text=mtext, hoverinfo="text", name="transfer"),
                    go.Scatter(x=[pos[n][0] for n in g.nodes], y=[pos[n][1] for n in g.nodes], mode="markers",
                               marker=dict(size=[22 if n == acct else 11 for n in g.nodes],
                                           color=[risk.get(n, 0) for n in g.nodes], colorscale="Reds", cmin=0, cmax=1,
                                           line=dict(width=1, color="#333"), colorbar=dict(title="risk")),
                               text=[label(n) for n in g.nodes], hoverinfo="text", name="account")])
                fig.update_layout(height=420, showlegend=False, margin=dict(l=0, r=0, t=0, b=0), annotations=arrows,
                                  xaxis=dict(visible=False), yaxis=dict(visible=False))
                st.plotly_chart(fig, width="stretch")
                st.caption("Arrows point from payer to payee (transfers on the learning days, the account's 40 largest "
                           "counterparties by USD). Hover a dot for its watchlist rank, or the middle of an arrow for "
                           "the amount. White dots are accounts we did not flag.")
                row = wl[wl.acct == acct].iloc[0]
                if has_split:
                    st.markdown("**Account half:** " + (
                        "graded - its own labels were never used to build the scorecard."
                        if row["split"] == "holdout" else
                        "learning - its labels helped set the points per red flag; its score still comes only "
                        "from its own money-flow patterns."))
                st.markdown(f"**Why flagged:** {row.reasons or 'channel risk only'}")
        sp = load("q2_signal_precision", variant)
        pc = load("q2_pattern_coverage", variant)
        c1, c2 = st.columns(2)
        if not sp.empty:
            c1.subheader("What each red flag is worth (weight of evidence)")
            flags = sp[sp.value > 0].sort_values("woe", ascending=False).copy()
            flags["red flag"] = [SIGNAL_NAMES.get(s, s) + (f" (level {int(v)})" if s == "segment" else "")
                                 for s, v in zip(flags.signal, flags.value)]
            flags["launderers among flagged"] = (flags.precision_calibration * 100).round(1).astype(str) + "%"
            c1.dataframe(flags[["red flag", "woe", "n_cal", "n_cal_bad", "launderers among flagged"]].round(3),
                         hide_index=True, width="stretch",
                         column_config={"woe": st.column_config.NumberColumn("points (WoE)", format="%.2f"),
                                        "n_cal": st.column_config.NumberColumn("accounts flagged"),
                                        "n_cal_bad": st.column_config.NumberColumn("of which launderers")})
            c1.caption("Learned on the learning half of the accounts, learning days only. More points = stronger "
                       "evidence; an account's score adds up the points of every flag it shows.")
        if not pc.empty:
            c2.subheader("Laundering typologies caught by the watchlist")
            c2.plotly_chart(px.bar(pc.sort_values("detection_rate"), x="detection_rate", y="pattern_type",
                                   orientation="h", range_x=[0, 1], text_auto=".0%", height=330,
                                   labels={"detection_rate": "share of schemes touched by the top-1,000 watchlist",
                                           "pattern_type": "scheme shape"}),
                            width="stretch")
            c2.caption(f"{int(pc.n_detected.sum()):,} of {int(pc.n_attempts.sum()):,} known schemes "
                       f"({pct(pc.n_detected.sum() / max(pc.n_attempts.sum(), 1), 0)}) touch at least one watchlist "
                       "account, across every shape, including shapes we have no detector for (account reuse). "
                       "Checked against IBM's answer key over all days and accounts: coverage, not the official grade.")

# ----------------------------------------------------------------------------------------------- live
with tab_live:
    st.header("Live monitor: batch insights applied to every new transaction")
    st.caption("Kafka topic `aml-transactions` (3 partitions, keyed by sender) → consumer applies the Q2 watchlist, "
               "Q1 hot segments and a 24-hour velocity window → alerts land here within milliseconds.")
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
            hide_index=True, width="stretch", height=400)
        c2.plotly_chart(px.histogram(a, y="rule", title="Alerts by rule", height=400), width="stretch")

# ----------------------------------------------------------------------------------------------- ops
with tab_ops:
    st.header("Pipeline & cost evidence")
    st.markdown("""
| Stage | AWS service | Why this choice (lecture reference) |
|---|---|---|
| Ingestion | EC2 + Kaggle API → **S3 raw zone** | data lake, schema-on-read (W4 slides 30-33) |
| Preparation | **Glue ETL** (Spark) → Parquet, partitioned by date | columnar + partitioned = less data scanned (W2 slides 70-73) |
| Catalog | **Glue crawler + Data Catalog** | metadata for Athena (W4 Glue lab) |
| Q1 analytics | **Athena** (serverless SQL, UNLOAD to Parquet) | pay per byte scanned (W5 slide 36) |
| Q2 analytics | **EMR** transient Spark cluster | in-memory multi-hop joins (W6 slides 28, 33) |
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
                st.dataframe(summ, hide_index=True, width="stretch")
                st.plotly_chart(px.bar(summ, x="test", y="data_scanned_mb", color="variant", barmode="group",
                                       log_y=True, title="MB scanned per query (log scale)"), width="stretch")
            st.subheader("All Athena queries run by the pipeline")
            st.dataframe(df.sort_values("at", ascending=False), hide_index=True, width="stretch")
    else:
        st.info("Cost/performance logs appear here in athena mode (s3://BUCKET/analytics/run_log/).")
