"""Minimal stand-ins for streamlit/plotly so the dashboard's data logic can be smoke-tested without them."""
import sys, types

class _Any:
    def __init__(self, *a, **k): pass
    def __call__(self, *a, **k): return _Any()
    def __getattr__(self, n): return _Any()
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def __iter__(self): return iter([_Any() for _ in range(5)])
    def __getitem__(self, i): return _Any()

class _St(types.ModuleType):
    def __getattr__(self, n):
        if n == "cache_data":
            return lambda *a, **k: (a[0] if a and callable(a[0]) else (lambda f: f))
        if n == "tabs": return lambda names: [_Any() for _ in names]
        if n == "columns": return lambda spec, **k: [_Any() for _ in range(spec if isinstance(spec, int) else len(spec))]
        if n == "selectbox": return lambda label, options, **k: list(options)[0]
        if n == "slider": return lambda label, lo, hi, val, *a, **k: val
        if n == "text_input": return lambda label, val="", **k: "cycle"
        if n == "toggle": return lambda label, value=False, **k: True
        if n == "button": return lambda *a, **k: False
        return _Any()

sys.modules["streamlit"] = _St("streamlit")
sys.modules["streamlit"].session_state = {}
for m in ["plotly", "plotly.express", "plotly.graph_objects"]:
    sys.modules[m] = _St(m)
sys.modules["plotly"].express = sys.modules["plotly.express"]
sys.modules["plotly"].graph_objects = sys.modules["plotly.graph_objects"]
