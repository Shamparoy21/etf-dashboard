# scripts/fetch_and_build.py
# Minimal builder: writes a valid data/data.json so we can confirm the workflow runs end-to-end.
# After this succeeds, we will replace this with the full live-data script.

import json
from pathlib import Path
from datetime import datetime, timezone

# Ensure folders exist
Path("data").mkdir(exist_ok=True)
Path("history").mkdir(exist_ok=True)

now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# Small test dataset (3 rows) so the dashboard renders rows from the workflow output
rows = [
    {
        "AMC": "Mirae Asset",
        "Category": "Index",
        "Scheme": "Nifty 50 ETF",
        "Symbol": "NIFTYETF",
        "iNAV": 271.9916,
        "Previous_NAV": 271.9960,
        "PremDiscPct": (271.9916/271.9960 - 1) * 100,
        "LTP": 272.00,
        "Volume": 100000,
        "PremLTPvsINAV": (272.00/271.9916 - 1) * 100,
        "iNAV_hist": [270.5, 271.2, 271.6, 271.8, 271.99],
        "Timestamp": now,
        "Source": "workflow-seed"
    },
    {
        "AMC": "Nippon India Mutual Fund",
        "Category": "Commodity",
        "Scheme": "Nippon India ETF Gold BeES",
        "Symbol": "GOLDBEES",
        "iNAV": 130.7379,
        "Previous_NAV": 130.1015,
        "PremDiscPct": (130.7379/130.1015 - 1) * 100,
        "LTP": 138.54,
        "Volume": 177836182,
        "PremLTPvsINAV": (138.54/130.7379 - 1) * 100,
        "iNAV_hist": [126.2, 127.0, 128.5, 129.8, 130.74],
        "Timestamp": now,
        "Source": "workflow-seed"
    },
    {
        "AMC": "Zerodha Fund House",
        "Category": "Commodity",
        "Scheme": "Zerodha Gold ETF",
        "Symbol": "GOLDCASE",
        "iNAV": 25.0305,
        "Previous_NAV": 24.9212,
        "PremDiscPct": (25.0305/24.9212 - 1) * 100,
        "LTP": 26.37,
        "Volume": 47254304,
        "PremLTPvsINAV": (26.37/25.0305 - 1) * 100,
        "iNAV_hist": [24.1, 24.4, 24.8, 25.0, 25.03],
        "Timestamp": now,
        "Source": "workflow-seed"
    }
]

# Write current data
with open("data/data.json", "w", encoding="utf-8") as f:
    json.dump(rows, f, ensure_ascii=False)

# Append a small history snapshot for sparklines (optional)
snap = {"timestamp": now, "records": [{"Symbol": r["Symbol"], "iNAV": r["iNAV"], "AMC": r["AMC"]} for r in rows]}
hist_path = Path("history") / f"snapshot_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
with open(hist_path, "w", encoding="utf-8") as f:
    json.dump(snap, f, ensure_ascii=False)

print("Built data/data.json and history snapshot.")
