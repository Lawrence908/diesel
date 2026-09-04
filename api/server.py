#!/usr/bin/env python3
"""Diesel crack spread API.

Pulls four daily price series from FRED's keyless CSV endpoint, splices the
distillate leg, computes the US (WTI) and Atlantic-basin (Brent) cracks, and
serves the result as one JSON document. Refreshed on a background timer and
cached to disk so a FRED outage cannot take the page down.
"""

import csv
import io
import json
import os
import threading
import time
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

FRED_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={}"

SERIES = {
    # id           purpose
    "DHOILNYH": "No. 2 heating oil spot, NY Harbor ($/gal), daily from 1986-06-02",
    "DDFUELNYH": "ULSD spot, NY Harbor ($/gal), daily from 2006-06-14",
    "DCOILWTICO": "WTI crude spot, Cushing OK ($/bbl), daily from 1986-01-02",
    "DCOILBRENTEU": "Brent crude spot, Europe ($/bbl), daily from 1987-05-20",
}

GAL_PER_BBL = 42
DATA_DIR = os.environ.get("DATA_DIR", "/app/data")
CACHE = os.path.join(DATA_DIR, "crack.json")
REFRESH_SECONDS = 12 * 3600

_state = {"payload": None, "fetched_at": None, "last_error": None}
_lock = threading.Lock()


def fetch_series(series_id):
    """Return {date_string: float} for one FRED series. Missing values dropped."""
    url = FRED_CSV.format(series_id)
    req = urllib.request.Request(url, headers={"User-Agent": "diesel.chrislawrence.ca"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        text = resp.read().decode("utf-8")
    out = {}
    reader = csv.reader(io.StringIO(text))
    header = next(reader)
    # FRED emits either "DATE,SERIES" or "observation_date,SERIES" depending on era.
    if len(header) < 2:
        raise ValueError("unexpected CSV header for %s: %r" % (series_id, header))
    for row in reader:
        if len(row) < 2:
            continue
        date, raw = row[0].strip(), row[1].strip()
        if raw in ("", "."):
            continue
        try:
            out[date] = float(raw)
        except ValueError:
            continue
    if not out:
        raise ValueError("no observations parsed for %s" % series_id)
    return out


def build_payload():
    raw = {sid: fetch_series(sid) for sid in SERIES}

    heat, ulsd = raw["DHOILNYH"], raw["DDFUELNYH"]
    wti, brent = raw["DCOILWTICO"], raw["DCOILBRENTEU"]

    # The distillate leg: ULSD where it exists (2006-06-14 on), No. 2 heating oil
    # before that. Same NY Harbor barge market, different sulfur spec.
    distillate_dates = set(heat) | set(ulsd)

    rows = []
    for date in sorted(distillate_dates):
        if date in ulsd:
            gal, leg = ulsd[date], "ulsd"
        else:
            gal, leg = heat[date], "heating_oil"
        per_bbl = gal * GAL_PER_BBL
        w = wti.get(date)
        b = brent.get(date)
        rows.append(
            {
                "d": date,
                "g": round(gal, 4),
                "leg": leg,
                "wti": w,
                "brent": b,
                "us": round(per_bbl - w, 3) if w is not None else None,
                "eu": round(per_bbl - b, 3) if b is not None else None,
            }
        )

    # Column-oriented: roughly half the bytes of a list of objects, and the
    # client wants parallel arrays anyway.
    payload = {
        "updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "splice_date": min(ulsd),
        "dates": [r["d"] for r in rows],
        "us": [r["us"] for r in rows],
        "eu": [r["eu"] for r in rows],
        "distillate": [r["g"] for r in rows],
        "wti": [r["wti"] for r in rows],
        "brent": [r["brent"] for r in rows],
        "series": SERIES,
    }
    return payload


def refresh():
    try:
        payload = build_payload()
    except Exception as exc:  # noqa: BLE001 - any failure falls back to cache
        with _lock:
            _state["last_error"] = "%s: %s" % (type(exc).__name__, exc)
        return False
    with _lock:
        _state["payload"] = payload
        _state["fetched_at"] = time.time()
        _state["last_error"] = None
    tmp = CACHE + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(payload, fh, separators=(",", ":"))
    os.chmod(tmp, 0o644)
    os.replace(tmp, CACHE)
    return True


def load_cache():
    if not os.path.exists(CACHE):
        return
    try:
        with open(CACHE) as fh:
            payload = json.load(fh)
    except Exception:  # noqa: BLE001 - a corrupt cache is not fatal
        return
    with _lock:
        _state["payload"] = payload
        _state["fetched_at"] = os.path.getmtime(CACHE)


def refresher():
    while True:
        time.sleep(REFRESH_SECONDS)
        refresh()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "diesel-api"

    def _send(self, code, body, ctype="application/json"):
        blob = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(blob)))
        self.send_header("Cache-Control", "public, max-age=1800")
        self.end_headers()
        self.wfile.write(blob)

    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler API
        path = self.path.split("?")[0]
        with _lock:
            payload = _state["payload"]
            fetched_at = _state["fetched_at"]
            last_error = _state["last_error"]

        if path == "/api/health":
            # Probe the dependency, not the process: no data means unhealthy.
            if payload is None:
                self._send(503, {"status": "no data", "error": last_error})
            else:
                self._send(
                    200,
                    {
                        "status": "ok",
                        "points": len(payload["dates"]),
                        "latest": payload["dates"][-1],
                        "fetched_at": fetched_at,
                        "last_error": last_error,
                    },
                )
            return

        if path == "/api/data":
            if payload is None:
                self._send(503, {"error": last_error or "no data yet"})
            else:
                self._send(200, json.dumps(payload, separators=(",", ":")).encode())
            return

        self._send(404, {"error": "not found"})

    def log_message(self, fmt, *args):
        print("%s - %s" % (self.address_string(), fmt % args), flush=True)


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    load_cache()
    refresh()
    threading.Thread(target=refresher, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()


if __name__ == "__main__":
    main()
