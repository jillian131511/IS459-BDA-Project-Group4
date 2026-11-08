"""Presentation helpers that reuse published analytics without changing scores (kept UI-free so they are testable)."""
import numpy as np
import pandas as pd


def wilson_lower_bound(k, n, z=1.96):
    """95% Wilson lower bound of k successes in n trials: the same formula athena/q1_segment_risk.sql uses to
    rank segments, so small routes with one lucky laundering transfer cannot top a ranking."""
    k, n = np.asarray(k, dtype=float), np.asarray(n, dtype=float)
    p = k / n
    return (p + z * z / (2 * n) - z * np.sqrt(p * (1 - p) / n + z * z / (4 * n * n))) / (1 + z * z / n)


def route_risk(corridors):
    """Adds wilson_lb (cautious laundering rate) and lb_lift (that rate as a multiple of the overall rate)."""
    data = corridors.copy()
    base = (data.laundering_rate / data.lift).median()          # lift = rate / overall rate, same for every row
    data["wilson_lb"] = wilson_lower_bound(data.n_laundering, data.n_txn)
    data["lb_lift"] = data.wilson_lb / base
    return data


def routes_export(corridors, by="risk"):
    """Bank-to-bank routes for download. rank_by_risk is always the Wilson lower-bound rank (ties: more transfers);
    by="value" orders the rows by laundered USD instead, to match the 'most laundered money' view."""
    data = corridors.sort_values(["wilson_lb", "n_txn"], ascending=[False, False], kind="stable").reset_index(drop=True)
    out = pd.DataFrame({"rank_by_risk": range(1, len(data) + 1), "from_bank": data.from_bank, "to_bank": data.to_bank,
                        "transfers": data.n_txn, "laundering_transfers": data.n_laundering,
                        "laundering_rate_pct": data.laundering_rate * 100, "lift": data.lift,
                        "lift_at_95pct_lower_bound": data.lb_lift, "total_usd": data.usd_volume,
                        "laundered_usd": data.laundered_usd})
    if by == "value":
        out = out.sort_values("laundered_usd", ascending=False, kind="stable").reset_index(drop=True)
    return out


def ranked_lift_data(data, dimensions):
    """Export exactly the given groups, ranked by lift (ties: more transfers first, then the dimensions)."""
    ranked = data.copy().sort_values(
        ["lift", "n_txn"] + dimensions, ascending=[False, False] + [True] * len(dimensions), kind="stable")
    ranked["laundering_rate_pct"] = ranked.laundering_rate * 100
    columns = dimensions + ["n_txn", "n_laundering", "laundering_rate_pct", "lift"]
    columns += [col for col in ("usd_volume", "laundered_usd") if col in ranked.columns]
    ranked = ranked[columns].reset_index(drop=True)
    ranked.insert(0, "rank_by_lift", range(1, len(ranked) + 1))
    return ranked


def corridor_preview(corridors, sender, max_routes=15):
    """One sender bank's qualifying routes, riskiest first. Returns (top routes, total route count)."""
    data = corridors.copy()
    for col in ("from_bank", "to_bank"):
        data[col] = data[col].astype(str)
    key = "wilson_lb" if "wilson_lb" in data.columns else "lift"   # worst-to-first by the lower bound when available
    routes = data[data.from_bank == str(sender)].sort_values(
        [key, "n_txn", "to_bank"], ascending=[False, False, True], kind="stable")
    return routes.head(max_routes), len(routes)
