"""Replay ordering regressions; no Kafka broker or AWS needed."""
import csv
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "streaming"))
from producer import COLS, ordered_events
from monitor_core import RiskModel


class ReplayOrder(unittest.TestCase):
    def test_shuffled_interval_preserves_rows_and_limits_after_sort(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "transactions.csv")
            timestamps = ["2022/09/10 12:00", "2022/09/08 00:01", "2022/09/07 23:59",
                          "2022/09/08 00:00", "2022/09/08 00:01", "2022/09/11 00:00"]
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(COLS)
                for i, ts in enumerate(timestamps):
                    writer.writerow([ts, "1", "A", "2", str(i), "1", "USD", "1", "USD", "ACH", "0"])
            events = list(ordered_events(path, "2022/09/08", "2022/09/11"))
            self.assertEqual([e["to_account"] for e in events], ["3", "1", "4", "0"])
            limited = list(ordered_events(path, "2022/09/08", "2022/09/11", 2))
            self.assertEqual(limited, events[:2])
            # Old day-8 recipients must expire before the day-10 transfer.
            model = RiskModel("acct,risk_probability\n", "payment_format,lift,wilson_lb\n", velocity_k=4)
            import time
            for ev in events:
                result = model.score(dict(src_id="1:A", dst_id=ev["to_account"],
                    ts_epoch=time.mktime(time.strptime(ev["ts_raw"], "%Y/%m/%d %H:%M")),
                    from_bank="1", to_bank="2", payment_format="ACH", payment_currency="USD",
                    receiving_currency="USD", amount_usd=1))
            self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
