"""
End-to-end local test: synthetic data -> ETL -> Q1 SQL -> Q2 SQL -> streaming monitor -> dashboard (stubbed UI).
Run: python -m unittest discover -s tests -p test_pipeline.py -v      (~1 minute, no AWS, no Spark needed)
"""
import json
import os
import runpy
import subprocess
import sys
import tempfile
import unittest

import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "glue"))
sys.path.insert(0, os.path.join(ROOT, "streaming"))
from patterns_parser import parse_patterns_text  # noqa: E402
from monitor_core import RiskModel, amount_band  # noqa: E402
sys.path.insert(0, os.path.join(ROOT, "dashboard"))
from presentation import corridor_preview, ranked_lift_data, route_risk, routes_export, wilson_lower_bound  # noqa: E402


def sh(*args):
    subprocess.run([sys.executable, *args], cwd=ROOT, check=True, capture_output=True)


class Pipeline(unittest.TestCase):
    def test_corridor_preview_and_ranked_export(self):
        data = pd.DataFrame({"from_bank": [1, 1, 2, 1], "to_bank": [2, 3, 1, 4],
                             "laundered_usd": [100, 10, 1, 5], "lift": [5, 20, 3, 2],
                             "n_txn": [1000, 50, 200, 100]})
        routes, total = corridor_preview(data, sender="1", max_routes=2)
        self.assertEqual(total, 3)
        self.assertEqual(routes.from_bank.tolist(), ["1", "1"])
        self.assertEqual(routes.to_bank.tolist(), ["3", "2"])          # direction kept, highest lift first
        self.assertEqual(len(data), 4)                                  # the source table is not modified
        exported = ranked_lift_data(data.assign(n_laundering=5, laundering_rate=0.1), ["from_bank", "to_bank"])
        self.assertEqual(exported.lift.tolist(), [20, 5, 3, 2])
        self.assertEqual(exported.rank_by_lift.tolist(), [1, 2, 3, 4])
        self.assertTrue((exported.laundering_rate_pct == 10).all())

    def test_route_ranking_uses_wilson_lower_bound(self):
        self.assertAlmostEqual(float(wilson_lower_bound(1, 50)), 0.0035, places=4)     # same formula as the SQL
        self.assertAlmostEqual(float(wilson_lower_bound(500, 50000)), 0.0092, places=4)
        # one lucky laundering transfer in 50 has the higher raw lift but must rank below 500 in 50,000
        data = pd.DataFrame({"from_bank": ["1", "1"], "to_bank": ["2", "3"], "n_txn": [50, 50000],
                             "n_laundering": [1, 500], "usd_volume": [1e4, 1e7], "laundered_usd": [100, 1e5]})
        data = data.assign(laundering_rate=data.n_laundering / data.n_txn,
                           lift=data.n_laundering / data.n_txn / 0.001)
        scored = route_risk(data)
        routes, total = corridor_preview(scored, sender="1")
        self.assertEqual(routes.to_bank.tolist(), ["3", "2"])
        self.assertTrue((scored.lb_lift < scored.lift).all())                          # cautious <= raw
        export = routes_export(scored)
        self.assertEqual(export.to_bank.tolist(), ["3", "2"])
        self.assertEqual(export.rank_by_risk.tolist(), [1, 2])
        by_value = routes_export(scored, by="value")
        self.assertEqual(by_value.laundered_usd.tolist(), [1e5, 100])                  # most laundered first
        self.assertEqual(by_value.rank_by_risk.tolist(), [1, 2])                       # risk rank kept

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()
        cls.raw, cls.cur, cls.ana = (os.path.join(cls.tmp, d) for d in ("raw", "curated", "analytics"))
        sh("local/make_synthetic_data.py", "--out", cls.raw, "--variant", "T", "--accounts", "4000",
           "--txns", "60000", "--attempts", "64")
        sh("local/etl_local.py", "--trans", f"{cls.raw}/T_Trans.csv", "--patterns", f"{cls.raw}/T_Patterns.txt",
           "--variant", "T", "--out", cls.cur)
        sh("local/run_local.py", "--curated", cls.cur, "--out", cls.ana, "--variant", "T")

    def test_patterns_parser(self):
        txt = ("BEGIN LAUNDERING ATTEMPT - FAN-OUT:  Max 2-degree Fan-Out\n"
               "2022/09/01 00:06,021174,800737690,012,80011F990,2848.96,Euro,2848.96,Euro,ACH,1\n"
               "2022/09/01 00:07,021174,800737690,013,80011F991,10.00,Euro,10.00,Euro,ACH,1\n"
               "END LAUNDERING ATTEMPT - FAN-OUT\n\n"
               "BEGIN LAUNDERING ATTEMPT - CYCLE:  Max 3 hops\n"
               "2022/09/02 00:06,1,A,2,B,1.00,US Dollar,1.00,US Dollar,Wire,1\n"
               "END LAUNDERING ATTEMPT - CYCLE\n")
        rows = list(parse_patterns_text(txt))
        self.assertEqual(len(rows), 3)
        self.assertEqual([r[1] for r in rows], ["FAN-OUT", "FAN-OUT", "CYCLE"])
        self.assertEqual(rows[2][0], 2)

    def test_etl_conserves_rows_and_labels(self):
        raw = pd.read_csv(f"{self.raw}/T_Trans.csv")
        tx = pd.read_csv(f"{self.cur}/transactions.csv")
        dq = pd.read_csv(f"{self.cur}/dq_report.csv").iloc[0]
        self.assertEqual(dq.rows_in, len(raw))
        self.assertEqual(dq.rows_out + dq.rows_malformed_dropped + dq.rows_duplicate_dropped, dq.rows_in)
        self.assertEqual(tx.is_laundering.sum(), raw["Is Laundering"].sum())
        self.assertEqual(tx.amount_usd.isna().sum(), 0)
        usd = tx[tx.payment_currency == "US Dollar"]
        self.assertTrue((abs(usd.amount_usd - usd.amount_paid) < 0.01).all())

    def test_q1_capture_curve_is_complete_and_monotonic(self):
        s = pd.read_csv(f"{self.ana}/q1_segment_risk.csv").sort_values("risk_rank")
        self.assertAlmostEqual(s.cum_txn_share.iloc[-1], 1.0, places=6)
        self.assertAlmostEqual(s.cum_laundering_share.iloc[-1], 1.0, places=6)
        self.assertTrue(s.cum_laundering_share.is_monotonic_increasing)
        self.assertTrue((s.wilson_lb <= s.laundering_rate + 1e-12).all())

    def test_q2_beats_random_on_holdout(self):
        e = pd.read_csv(f"{self.ana}/q2_evaluation.csv").set_index("k")
        self.assertGreater(e.loc[100, "precision_combined"], 3 * e.loc[100, "base_rate"])
        w = pd.read_csv(f"{self.ana}/q2_watchlist.csv")
        self.assertTrue(w.risk_probability.between(0, 1).all())
        self.assertTrue(w.risk_rank.is_monotonic_increasing)

    def test_q2_time_based_grading(self):
        """Learn on early days, grade on later days (docs/decisions.md): both periods present and sane."""
        out = os.path.join(self.tmp, "analytics_time")
        sh("local/run_local.py", "--curated", self.cur, "--out", out, "--variant", "T",
           "--train-until", "2022-09-08", "--test-until", "2022-09-10")
        e = pd.read_csv(f"{out}/q2_evaluation_time.csv")
        self.assertEqual(set(e.period), {"official", "incl_tail"})
        self.assertTrue(e.precision_combined.between(0, 1).all())
        off = e[e.period == "official"].set_index("k")
        tail = e[e.period == "incl_tail"].set_index("k")
        # the wider period can only contain the same or more laundering accounts
        self.assertTrue((tail.n_holdout_bad >= off.n_holdout_bad).all())

    def test_watchlist_trail_coverage(self):
        """Money trails stay bounded to the top-300 accounts, 40 flows each, and include each one's largest flows."""
        w = pd.read_csv(f"{self.ana}/q2_watchlist.csv")
        tx = pd.read_csv(f"{self.cur}/transactions.csv")
        pairs = tx[tx.is_self == 0].groupby(["src_id", "dst_id"]).amount_usd.sum()
        published = pd.read_csv(f"{self.ana}/q2_watchlist_edges.csv")
        actual = set(zip(published.src_id, published.dst_id))
        self.assertLessEqual(len(published), 300 * 40)
        for acct in w[w.risk_rank <= 300].acct:
            incident = pairs[(pairs.index.get_level_values(0) == acct) | (pairs.index.get_level_values(1) == acct)]
            cutoff = incident.nlargest(40).min()             # tied boundary amounts may pick either edge
            self.assertTrue(set(incident[incident > cutoff].index) <= actual, acct)
            self.assertTrue(any(acct in edge for edge in actual), acct)

    def test_streaming_rules(self):
        wl = "acct,risk_rank,risk_probability,reasons\n1:A,1,0.9,Part of 1 money cycle\n"
        seg = ("payment_format,payment_currency,is_cross_bank,is_cross_currency,amount_band,lift,wilson_lb\n"
               "ACH,US Dollar,1,0,d 10K-100K,50,0.1\nWire,Euro,1,0,b 100-1K,3,0.001\n")
        m = RiskModel(wl, seg, velocity_k=3)
        base = dict(dst_id="2:B", from_bank="1", to_bank="2", payment_currency="US Dollar",
                    receiving_currency="US Dollar", payment_format="Cheque", amount_usd=50.0, ts_epoch=0)
        self.assertIsNone(m.score({**base, "src_id": "9:Z"}))                          # nothing risky
        self.assertIsNone(m.score({**base, "src_id": "1:A"}))                          # watchlisted, low-risk txn
        hot = m.score({**base, "src_id": "9:Y", "payment_format": "ACH", "amount_usd": 20000.0})
        self.assertIn("high-risk segment", hot["reasons"])
        for i in range(3):                                                             # velocity window
            r = m.score({**base, "src_id": "7:V", "dst_id": f"2:{i}", "ts_epoch": i * 60})
        self.assertIn("velocity", r["reasons"])
        h = RiskModel(wl, seg, velocity_k=3, hubs=["7:V"])                                 # hubs skip velocity
        for i in range(3):
            r = h.score({**base, "src_id": "7:V", "dst_id": f"2:{i}", "ts_epoch": i * 60})
        self.assertIsNone(r)
        self.assertEqual(amount_band(999.99), "b 100-1K")

    def test_dashboard_runs(self):
        sys.path.insert(0, os.path.join(ROOT, "tests", "stubs"))
        import make_stubs  # noqa: F401
        os.environ["LOCAL_DIR"] = self.ana
        app = runpy.run_path(os.path.join(ROOT, "dashboard", "app.py"), run_name="__main__")
        import streamlit as st
        # BQ1 bank routes: clicking a receiver that also sends opens it as the sender; anything else only explains
        receiver = app["senders"][-1]
        st.session_state[app["route_chart_key"]] = {"selection": {"points": [{"customdata": receiver}]}}
        app["open_receiver_as_sender"]()
        self.assertEqual(st.session_state[app["sender_key"]], receiver)
        st.session_state[app["route_chart_key"]] = {"selection": {"points": [{"customdata": "no-such-bank"}]}}
        app["open_receiver_as_sender"]()
        self.assertEqual(st.session_state[app["sender_key"]], receiver)
        self.assertIn("nothing to open", st.session_state[app["route_notice_key"]])

    def test_sql_templates_render(self):
        sys.path.insert(0, os.path.join(ROOT, "emr"))
        from network_sql import DEFAULT_PARAMS, STEPS, render
        p = dict(DEFAULT_PARAMS, fan_out_k=5, fan_in_k=5, hub_degree=50)
        for name, sql in STEPS:
            self.assertNotIn("{", render(sql, p), name)
        asl = json.load(open(os.path.join(ROOT, "stepfunctions", "aml_pipeline.asl.json")))
        self.assertEqual(asl["StartAt"], "Defaults")


if __name__ == "__main__":
    unittest.main()
