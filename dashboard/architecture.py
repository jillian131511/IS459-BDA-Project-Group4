"""
Architecture diagram for the dashboard, drawn as SVG so it follows the light / dark theme.

Styled as an investigation board: case cards, a red thread for the batch money trail, a dashed thread for
the live speed layer, and the platform icon in each card's corner. Icons: official AWS Architecture Icons
(AWS permits their use in architecture diagrams) and Simple Icons (CC0) for Kafka, Streamlit and Kaggle.

    python dashboard/architecture.py docs/          # writes architecture_light.svg and architecture_dark.svg
"""
import base64
import os
import sys
from html import escape

ICON_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "icons")
W, H = 1540, 900

THEMES = {
    "dark": dict(page="#0e1117", card="#161b26", inner="#1c2230", border="#2b3242", ink="#f0efec", ink2="#a9adb8",
                 muted="#7d818c", thread="#e5484d", band="#1d1a2b", band_border="#3b3355", bq1="#3987e5",
                 bq2="#d95926", mono_icon="#f0efec"),
    "light": dict(page="#f7f6f3", card="#ffffff", inner="#f4f3ef", border="#dcdad3", ink="#1a1a19", ink2="#55544f",
                  muted="#8a8984", thread="#c8372d", band="#f1eef9", band_border="#cfc6ea", bq1="#2a78d6",
                  bq2="#eb6834", mono_icon="#1a1a19"),
}
BRAND = {"streamlit": "#FF4B4B", "kaggle": "#20BEFF"}           # single-colour icons keep their brand colour
FONT = 'system-ui, -apple-system, Segoe UI, Roboto, sans-serif'
MONO = 'ui-monospace, SFMono-Regular, Consolas, monospace'


def _icon(name: str, c: dict) -> str:
    path = os.path.join(ICON_DIR, f"{name}.svg")
    if not os.path.exists(path):
        return ""
    svg = open(path, encoding="utf-8").read()
    if "<?xml" not in svg:                                       # Simple Icons: tint the single-colour glyph
        svg = svg.replace("<svg ", f'<svg fill="{BRAND.get(name, c["mono_icon"])}" ', 1)
    return "data:image/svg+xml;base64," + base64.b64encode(svg.encode("utf-8")).decode("ascii")


def _text(x, y, s, size=13, colour="#000", weight=400, family=FONT, anchor="start", spacing=None):
    ls = f' letter-spacing="{spacing}"' if spacing else ""
    return (f'<text x="{x}" y="{y}" font-family="{family}" font-size="{size}" font-weight="{weight}" '
            f'fill="{colour}" text-anchor="{anchor}"{ls}>{escape(s)}</text>')


def _card(c, x, y, w, h, title, lines, icon=None, tag=None, tag_colour=None, fill=None, mono_title=False):
    out = [f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{fill or c["card"]}" '
           f'stroke="{c["border"]}" stroke-width="1"/>']
    tx = x + 16
    if tag:
        tw = 9 * len(tag) + 14
        out.append(f'<rect x="{tx}" y="{y + 14}" width="{tw}" height="20" rx="10" fill="{tag_colour}"/>')
        out.append(_text(tx + tw / 2, y + 28, tag, 11, "#ffffff", 700, anchor="middle", spacing="0.5"))
        tx += tw + 8
    out.append(_text(tx, y + 29, title, 15, c["ink"], 650, MONO if mono_title else FONT))
    for i, line in enumerate(lines):
        out.append(_text(x + 16, y + 54 + 18 * i, line, 12.5, c["ink2"]))
    if icon:
        uri = _icon(icon, c)
        if uri:
            out.append(f'<image href="{uri}" x="{x + w - 40}" y="{y + 10}" width="28" height="28"/>')
    return "\n".join(out)


def _thread(c, pts, dashed=False, label=None, label_at=None):
    d = "M " + " L ".join(f"{px} {py}" for px, py in pts)
    dash = ' stroke-dasharray="7 6"' if dashed else ""
    out = [f'<path d="{d}" fill="none" stroke="{c["thread"]}" stroke-width="2" stroke-linejoin="round"{dash} '
           f'marker-end="url(#arrow)"/>',
           f'<circle cx="{pts[0][0]}" cy="{pts[0][1]}" r="3.5" fill="{c["thread"]}"/>']          # the pin
    if label:
        lx, ly = label_at
        out.append(_text(lx, ly, label, 11.5, c["muted"]))
    return "\n".join(out)


def architecture_svg(mode: str = "dark") -> str:
    c = THEMES["light" if mode == "light" else "dark"]
    o = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}">',
         f'<defs><marker id="arrow" viewBox="0 0 10 10" refX="8.5" refY="5" markerWidth="7" markerHeight="7" '
         f'orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="{c["thread"]}"/></marker></defs>',
         f'<rect width="{W}" height="{H}" rx="14" fill="{c["page"]}"/>',
         _text(40, 52, "Follow the Money", 26, c["ink"], 750),
         _text(40, 78, "How a raw transfer becomes a case an investigator can open, end to end on AWS",
               14, c["ink2"])]

    # orchestration band
    o.append(f'<rect x="300" y="100" width="1200" height="48" rx="10" fill="{c["band"]}" '
             f'stroke="{c["band_border"]}" stroke-width="1"/>')
    uri = _icon("stepfunctions", c)
    if uri:
        o.append(f'<image href="{uri}" x="314" y="110" width="28" height="28"/>')
    o.append(_text(352, 130, "AWS Step Functions", 15, c["ink"], 650))
    o.append(_text(510, 130, "One click runs the whole case: Glue ETL, crawler, BQ1, then BQ2 and the cost "
                             "benchmark in parallel. Retries each step and always shuts the EMR cluster down.",
                   12.5, c["ink2"]))

    # stage headings
    for x, label in [(40, "01  COLLECT"), (300, "02  STORE"), (640, "03  PREPARE"), (950, "04  INVESTIGATE"),
                     (1270, "05  ACT")]:
        o.append(_text(x, 186, label, 12, c["muted"], 700, spacing="1.6"))

    # 01 collect
    o.append(_card(c, 40, 200, 230, 112, "IBM AML on Kaggle",
                   ["Transactions: 5M to 180M rows", "Patterns file: the answer key", "HI-Small to build, HI-Large final"],
                   icon="kaggle"))
    o.append(_card(c, 40, 352, 230, 96, "EC2 ingestion",
                   ["Streams Kaggle straight to S3", "md5 and row-count manifest", "then shuts itself down"],
                   icon="ec2"))

    # 02 store
    o.append(f'<rect x="300" y="200" width="300" height="460" rx="10" fill="{c["card"]}" '
             f'stroke="{c["border"]}" stroke-width="1"/>')
    o.append(_text(316, 229, "S3 data lake", 15, c["ink"], 650))
    uri = _icon("s3", c)
    if uri:
        o.append(f'<image href="{uri}" x="560" y="210" width="28" height="28"/>')
    zones = [("raw/", ["CSV and patterns as delivered", "read as-is, never edited"]),
             ("curated/", ["Parquet, partitioned by day", "USD amounts, DQ report, FX rates"]),
             ("analytics/", ["BQ1 risk, BQ2 watchlist", "grading results, query cost log"]),
             ("streaming/alerts/", ["live alerts in micro-batches"])]
    for i, (name, lines) in enumerate(zones):
        o.append(_card(c, 314, 250 + 100 * i, 272, 88, name, lines, fill=c["inner"], mono_title=True))

    # 03 prepare
    o.append(_card(c, 640, 250, 270, 112, "Glue ETL (Spark)",
                   ["Fixed schema, duplicates removed", "Data-quality report", "FX to USD from the data itself"],
                   icon="glue"))
    o.append(_card(c, 640, 392, 270, 76, "Glue crawler and catalog", ["Tables and day partitions for Athena"],
                   icon="glue"))

    # 04 investigate
    o.append(_card(c, 950, 200, 290, 150, "Where is the risk?",
                   ["Amazon Athena, serverless SQL", "Risk lift per channel, Wilson bound", "Capture curve for any budget",
                    "Hour profile, bank corridors"], icon="athena", tag="BQ1", tag_colour=c["bq1"]))
    o.append(_card(c, 950, 380, 290, 168, "Who is moving it?",
                   ["Amazon EMR, Spark on a temporary cluster", "Fan-in, fan-out, 2 to 4 hop cycles",
                    "Scatter-gather, gather-scatter", "Scorecard with plain-English reasons",
                    "Graded on unseen accounts, later days"], icon="emr", tag="BQ2", tag_colour=c["bq2"]))

    # 05 act
    o.append(_card(c, 1270, 200, 230, 348, "Investigator dashboard",
                   ["Alert-budget optimiser", "Risk heatmaps", "Watchlist with reasons", "Money-trail explorer",
                    "Suggested case", "Live alert feed", "Cost and benchmark evidence", "",
                    "Used by", "AML monitoring team (BQ1)", "Financial-crime", "investigators (BQ2)"],
                   icon="streamlit"))

    # speed layer
    o.append(f'<rect x="640" y="700" width="860" height="150" rx="10" fill="none" stroke="{c["thread"]}" '
             f'stroke-width="1" stroke-dasharray="4 5" opacity="0.7"/>')
    o.append(_text(656, 726, "LIVE SPEED LAYER", 12, c["thread"], 700, spacing="1.6"))
    o.append(_card(c, 656, 742, 250, 92, "Producer", ["Replays days the batch never saw", "keyed by sender account"]))
    o.append(_card(c, 936, 742, 250, 92, "Kafka on EC2", ["topic aml-transactions", "3 partitions, consumer group"],
                   icon="kafka"))
    o.append(_card(c, 1216, 742, 270, 92, "Consumer", ["Scores each transfer in milliseconds", "watchlist, hot channel, velocity"]))

    # the red thread: batch money trail
    o.append(_thread(c, [(155, 312), (155, 350)]))                                          # Kaggle -> EC2
    o.append(_thread(c, [(270, 400), (298, 400), (298, 300), (312, 300)]))                  # EC2 -> raw/
    o.append(_thread(c, [(586, 294), (638, 294)]))                                          # raw/ -> Glue
    o.append(_thread(c, [(640, 340), (618, 340), (618, 394), (588, 394)]))                  # Glue -> curated/
    o.append(_thread(c, [(775, 362), (775, 390)]))                                          # Glue -> crawler
    o.append(_thread(c, [(910, 430), (928, 430), (928, 276), (948, 276)]))                  # catalog -> BQ1
    o.append(_thread(c, [(1095, 350), (1095, 378)], label="channel risk", label_at=(1112, 369)))  # BQ1 -> BQ2
    o.append(_thread(c, [(950, 494), (588, 494)], label="results", label_at=(840, 486)))
    o.append(_thread(c, [(1240, 260), (1268, 260)]))                                        # Athena -> dashboard

    # dashed thread: live layer
    o.append(_thread(c, [(314, 322), (280, 322), (280, 872), (781, 872), (781, 836)], dashed=True,
                     label="replays raw transfers from days the batch never learned from", label_at=(292, 864)))
    o.append(_thread(c, [(906, 788), (934, 788)], dashed=True))
    o.append(_thread(c, [(1186, 788), (1214, 788)], dashed=True))
    o.append(_thread(c, [(1400, 742), (1400, 550)], dashed=True, label="live alerts", label_at=(1408, 650)))
    o.append(_thread(c, [(1250, 742), (1250, 680), (612, 680), (612, 594), (588, 594)], dashed=True))

    # key
    o.append(f'<line x1="40" y1="700" x2="80" y2="700" stroke="{c["thread"]}" stroke-width="2"/>')
    o.append(_text(90, 704, "Batch trail, daily, one click", 12, c["ink2"]))
    o.append(f'<line x1="40" y1="726" x2="80" y2="726" stroke="{c["thread"]}" stroke-width="2" '
             f'stroke-dasharray="7 6"/>')
    o.append(_text(90, 730, "Live trail, each new transfer", 12, c["ink2"]))
    o.append("</svg>")
    return "\n".join(o)


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "."
    for m in ("light", "dark"):
        with open(os.path.join(out, f"architecture_{m}.svg"), "w", encoding="utf-8") as f:
            f.write(architecture_svg(m))
    print("written to", out)
