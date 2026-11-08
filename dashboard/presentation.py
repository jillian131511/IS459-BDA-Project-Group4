"""Presentation helpers that reuse published analytics without changing scores (kept UI-free so they are testable)."""
import pandas as pd


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
    """One sender bank's qualifying routes, highest lift first. Returns (top routes, total route count)."""
    data = corridors.copy()
    for col in ("from_bank", "to_bank"):
        data[col] = data[col].astype(str)
    routes = data[data.from_bank == str(sender)].sort_values(
        ["lift", "n_txn", "to_bank"], ascending=[False, False, True], kind="stable")
    return routes.head(max_routes), len(routes)
