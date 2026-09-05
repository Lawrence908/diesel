#!/usr/bin/env python3
"""Record when EIA actually publishes a new daily spot observation.

Polls the newest period for each daily leg and appends a line to a log
whenever any series advances. Run it hourly; after a week the log shows the
real release cadence -- which weekday, and what time of day -- with no
guessing from documentation that may not match practice.

    ./eia-watch.py            # one poll, append if changed
    ./eia-watch.py --show     # print what has been observed so far
"""

import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ENV = os.path.join(HERE, "..", ".env")
LOG = os.path.join(HERE, "..", "data", "eia-releases.jsonl")

SERIES = [
    "RWTC",
    "RBRTE",
    "EER_EPD2F_PF4_Y35NY_DPG",
    "EER_EPD2DXL0_PF4_Y35NY_DPG",
    "EER_EPD2DXL0_PF4_RGC_DPG",
    "EER_EPMRU_PF4_Y35NY_DPG",
]


def api_key():
    for line in open(ENV):
        if line.startswith("EIA_API_KEY="):
            return line.split("=", 1)[1].strip()
    raise SystemExit("no EIA_API_KEY in %s" % ENV)


def newest_periods(key):
    """Return {series_id: newest period} in one request per series."""
    out = {}
    for sid in SERIES:
        params = [
            ("api_key", key), ("frequency", "daily"), ("data[0]", "value"),
            ("facets[series][]", sid),
            ("sort[0][column]", "period"), ("sort[0][direction]", "desc"),
            ("length", "1"),
        ]
        url = "https://api.eia.gov/v2/petroleum/pri/spt/data/?%s" % urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"User-Agent": "diesel eia-watch"})
        rows = json.loads(urllib.request.urlopen(req, timeout=60).read().decode())
        rows = rows["response"]["data"]
        if rows:
            out[sid] = rows[0]["period"]
    return out


def last_seen():
    if not os.path.exists(LOG):
        return None
    prev = None
    for line in open(LOG):
        line = line.strip()
        if line:
            try:
                prev = json.loads(line)
            except ValueError:
                continue
    return prev


def show():
    if not os.path.exists(LOG):
        print("no observations logged yet")
        return
    print("%-26s %s" % ("observed at (UTC)", "newest period per series"))
    for line in open(LOG):
        line = line.strip()
        if not line:
            continue
        rec = json.loads(line)
        when = rec["observed_at"]
        pt = rec.get("observed_at_pacific", "")
        periods = rec["periods"]
        uniq = sorted(set(periods.values()))
        print("%-26s %-24s %s" % (when, pt, ",".join(uniq)))


def main():
    if "--show" in sys.argv:
        show()
        return

    key = api_key()
    periods = newest_periods(key)
    prev = last_seen()

    if prev and prev.get("periods") == periods:
        print("no change (newest still %s)" % sorted(set(periods.values())))
        return

    now = datetime.now(timezone.utc)
    rec = {
        "observed_at": now.isoformat(timespec="seconds"),
        "observed_at_pacific": now.astimezone().strftime("%Y-%m-%d %H:%M %Z"),
        "periods": periods,
    }
    with open(LOG, "a") as fh:
        fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
    print("ADVANCED -> %s at %s" % (sorted(set(periods.values())), rec["observed_at_pacific"]))


if __name__ == "__main__":
    main()
