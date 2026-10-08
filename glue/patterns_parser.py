"""Pure-python parser for <variant>_Patterns.txt (shared by the Glue job and local tests).

File layout (AMLworld / IBM AML on Kaggle):
    BEGIN LAUNDERING ATTEMPT - FAN-OUT:  Max 16-degree Fan-Out
    2022/09/01 00:06,021174,800737690,012,80011F990,2848.96,Euro,2848.96,Euro,ACH,1
    ...
    END LAUNDERING ATTEMPT - FAN-OUT
"""
import re

BEGIN_RE = re.compile(r"^BEGIN LAUNDERING ATTEMPT\s*-\s*([A-Z][A-Z\- ]*?)\s*(?::\s*(.*))?$")


def parse_patterns_text(text):
    """Yield (attempt_no, pattern_type, detail, csv_line) for every transaction inside an attempt."""
    attempt, ptype, detail = 0, None, ""
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        m = BEGIN_RE.match(line)
        if m:
            attempt += 1
            ptype, detail = m.group(1).strip(), (m.group(2) or "").strip()
        elif line.startswith("END LAUNDERING ATTEMPT"):
            ptype = None
        elif ptype is not None and line.count(",") >= 10:
            yield attempt, ptype, detail, line
