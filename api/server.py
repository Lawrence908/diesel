#!/usr/bin/env python3
"""Diesel crack spread API.

Primary source is the EIA API v2 (the original publisher). FRED's keyless CSV
endpoint is the fallback: it redistributes the same EIA spot series, so a FRED
run produces identical numbers for the four core series but cannot supply the
Gulf Coast leg, the 3-2-1 crack, or the pre-1986 monthly history.

Everything is cached to disk, so an outage at either source cannot blank the
page.
"""

import csv
import io
import json
import os
import re
import threading
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

EIA_KEY = os.environ.get("EIA_API_KEY", "").strip()
EIA_BASE = "https://api.eia.gov/v2"
FRED_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={}"
# EIA states its own release dates here, holiday shifts already applied. The
# API itself carries no publication timestamp, so this page is the only
# authoritative answer to "when does the next batch land".
EIA_SPOT_PAGE = "https://www.eia.gov/dnav/pet/pet_pri_spt_s1_d.htm"
UA = {"User-Agent": "diesel.chrislawrence.ca (crack spread chart)"}

# Daily spot legs. EIA series id -> (short key, FRED fallback id or None).
DAILY = {
    "RWTC": ("wti", "DCOILWTICO"),
    "RBRTE": ("brent", "DCOILBRENTEU"),
    "EER_EPD2F_PF4_Y35NY_DPG": ("heat_ny", "DHOILNYH"),
    "EER_EPD2DXL0_PF4_Y35NY_DPG": ("ulsd_ny", "DDFUELNYH"),
    "EER_EPD2DXL0_PF4_RGC_DPG": ("ulsd_gc", None),
    "EER_EPMRU_PF4_Y35NY_DPG": ("gaso_ny", None),
}

# Monthly deep history. These are a DIFFERENT price basis to the spot legs
# above (refiner wholesale, and an average acquisition cost rather than a spot
# marker), so they are reported as their own series and never spliced in.
MONTHLY = {
    "refoth": ("EMA_EPD2_PWG_NUS_DPG", "dist_wholesale"),
    "rac2": ("R0000____3", "rac"),
}

SOURCE_NOTES = {
    "RWTC": "Cushing, OK WTI spot FOB ($/bbl), daily from 1986-01-02",
    "RBRTE": "Europe Brent spot FOB ($/bbl), daily from 1987-05-20",
    "EER_EPD2F_PF4_Y35NY_DPG": "NY Harbor No. 2 heating oil spot FOB ($/gal), daily from 1986-06-02",
    "EER_EPD2DXL0_PF4_Y35NY_DPG": "NY Harbor ULSD spot ($/gal), daily from 2006-06-14",
    "EER_EPD2DXL0_PF4_RGC_DPG": "US Gulf Coast ULSD spot ($/gal), daily from 2006-06-14",
    "EER_EPMRU_PF4_Y35NY_DPG": "NY Harbor conventional regular gasoline spot FOB ($/gal), daily from 1986-06-02",
    "EMA_EPD2_PWG_NUS_DPG": "US No. 2 distillate wholesale/resale price by refiners ($/gal), monthly 1983-01 to 2022-03",
    "R0000____3": "US crude oil composite acquisition cost by refiners ($/bbl), monthly from 1974-01",
}

GAL_PER_BBL = 42
EIA_PAGE = 5000  # the API's hard JSON row cap
DATA_DIR = os.environ.get("DATA_DIR", "/app/data")
CACHE = os.path.join(DATA_DIR, "crack.json")
REVISIONS = os.path.join(DATA_DIR, "revisions.jsonl")
REVISIONS_META = os.path.join(DATA_DIR, "revisions-meta.json")

# Raw published legs worth watching for restatement. Derived values (the
# cracks) are not tracked: they move only because a leg moved, so tracking
# them would report the same restatement several times over.
DAILY_TRACKED = [
    ("distillate", "NY Harbor distillate ($/gal)"),
    ("gulf_distillate", "Gulf Coast ULSD ($/gal)"),
    ("gasoline", "NY Harbor gasoline ($/gal)"),
    ("wti", "WTI ($/bbl)"),
    ("brent", "Brent ($/bbl)"),
]
MONTHLY_TRACKED = [
    ("distillate", "Refiner distillate wholesale ×42 ($/bbl)"),
    ("rac", "Refiner crude acquisition cost ($/bbl)"),
]
EPSILON = 1e-9          # float-parse noise, not a real restatement
REVISIONS_IN_PAYLOAD = 60  # most recent records embedded for the page

# EIA publishes these daily series in one weekly batch, normally Wednesday
# "after 1:00 p.m. eastern" (17:00 UTC in EDT, 18:00 in EST), slipping to
# Thursday in weeks with a Monday federal holiday. Rather than track that
# calendar here, we simply check at fixed times of day that sit an hour or so
# past the release window; the first slot catches a normal Wednesday, the
# second covers a late or holiday-shifted post. Anchoring to wall clock also
# means a redeploy no longer drifts the schedule.
REFRESH_AT_UTC = ("18:15", "23:15")
RETRY_BASE_SECONDS = 60      # first retry after a failure
RETRY_MAX_SECONDS = 1800     # ceiling, so a long outage settles at 30 min
RETRY_MAX_DOUBLINGS = 10     # keeps the shift bounded regardless of streak length

_state = {"payload": None, "fetched_at": None, "last_error": None, "failures": 0, "next_attempt": None}
_lock = threading.Lock()


def _get(url):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=90) as resp:
        return resp.read().decode("utf-8")


def eia_series(route, series_id, frequency):
    """Return {period: float} for one EIA series, paginating past the 5000-row cap."""
    out = {}
    offset = 0
    while True:
        params = [
            ("api_key", EIA_KEY),
            ("frequency", frequency),
            ("data[0]", "value"),
            ("facets[series][]", series_id),
            ("sort[0][column]", "period"),
            ("sort[0][direction]", "asc"),
            ("offset", str(offset)),
            ("length", str(EIA_PAGE)),
        ]
        url = "%s/petroleum/pri/%s/data/?%s" % (EIA_BASE, route, urllib.parse.urlencode(params))
        body = json.loads(_get(url))
        rows = body.get("response", {}).get("data", [])
        for row in rows:
            raw = row.get("value")
            if raw in (None, "", "."):
                continue
            try:
                out[row["period"]] = float(raw)
            except (TypeError, ValueError):
                continue
        if len(rows) < EIA_PAGE:
            break
        offset += EIA_PAGE
    if not out:
        raise ValueError("no observations for %s" % series_id)
    return out


def fred_series(series_id):
    """Return {date: float} for one FRED series (keyless CSV)."""
    out = {}
    reader = csv.reader(io.StringIO(_get(FRED_CSV.format(series_id))))
    next(reader)
    for row in reader:
        if len(row) < 2 or row[1].strip() in ("", "."):
            continue
        try:
            out[row[0].strip()] = float(row[1].strip())
        except ValueError:
            continue
    if not out:
        raise ValueError("no observations for %s" % series_id)
    return out


def _mdy_after(text, label):
    """Find `label: M/D/YYYY` in tag-stripped page text, as an ISO date."""
    match = re.search(label + r":\s*(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if not match:
        return None
    month, day, year = (int(g) for g in match.groups())
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return None


def fetch_release_dates():
    """(last_release, next_release) straight from EIA's spot price page.

    Optional: a failure here costs the page its "next release" line but must
    never sink a refresh that otherwise succeeded.
    """
    try:
        text = re.sub(r"<[^>]*>", " ", _get(EIA_SPOT_PAGE))
    except Exception as exc:  # noqa: BLE001 - decorative, never fatal
        print("release-date fetch failed: %s: %s" % (type(exc).__name__, exc), flush=True)
        return None, None
    # "Release Date" also occurs inside "Next Release Date", so anchor the
    # first lookup to a position that is not preceded by "Next ".
    return _mdy_after(text, r"(?<!Next )Release Date"), _mdy_after(text, r"Next Release Date")


def fetch_daily():
    """Fetch every daily leg. Returns (legs, source_name)."""
    if EIA_KEY:
        try:
            return {key: eia_series("spt", sid, "daily") for sid, (key, _) in DAILY.items()}, "eia"
        except Exception as exc:  # noqa: BLE001 - fall back rather than fail
            print("EIA daily fetch failed (%s: %s); falling back to FRED" % (type(exc).__name__, exc), flush=True)
    legs = {}
    for _sid, (key, fred_id) in DAILY.items():
        if fred_id:
            legs[key] = fred_series(fred_id)
    return legs, "fred"


def fetch_monthly():
    """Deep monthly history. EIA only — there is no keyless equivalent."""
    if not EIA_KEY:
        return None
    try:
        dist = eia_series("refoth", MONTHLY["refoth"][0], "monthly")
        rac = eia_series("rac2", MONTHLY["rac2"][0], "monthly")
    except Exception as exc:  # noqa: BLE001 - the deep panel is optional
        print("EIA monthly fetch failed: %s: %s" % (type(exc).__name__, exc), flush=True)
        return None
    periods = sorted(set(dist) | set(rac))
    return {
        "periods": periods,
        "crack": [
            round(dist[p] * GAL_PER_BBL - rac[p], 3) if p in dist and p in rac else None
            for p in periods
        ],
        "distillate": [round(dist[p] * GAL_PER_BBL, 3) if p in dist else None for p in periods],
        "rac": [rac.get(p) for p in periods],
    }


def build_payload():
    legs, source = fetch_daily()

    heat, ulsd = legs["heat_ny"], legs["ulsd_ny"]
    wti, brent = legs["wti"], legs["brent"]
    gulf = legs.get("ulsd_gc", {})
    gaso = legs.get("gaso_ny", {})

    dates = sorted(set(heat) | set(ulsd))

    def crack(distillate_bbl, crude):
        return round(distillate_bbl - crude, 3) if distillate_bbl is not None and crude is not None else None

    payload = {
        "updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": source,
        "splice_date": min(ulsd),
        "dates": dates,
        "us": [], "eu": [], "gulf": [], "c321": [],
        "distillate": [], "gulf_distillate": [], "gasoline": [],
        "wti": [], "brent": [],
        "series": SOURCE_NOTES if source == "eia" else {
            k: v for k, v in SOURCE_NOTES.items() if k in ("RWTC", "RBRTE",
                "EER_EPD2F_PF4_Y35NY_DPG", "EER_EPD2DXL0_PF4_Y35NY_DPG")
        },
    }

    for date in dates:
        # The distillate leg: ULSD where it exists (2006-06-14 on), No. 2
        # heating oil before that. Same NY Harbor barge market, different
        # sulfur spec.
        gal = ulsd[date] if date in ulsd else heat[date]
        dist_bbl = gal * GAL_PER_BBL
        w, b = wti.get(date), brent.get(date)
        g = gulf.get(date)
        gas = gaso.get(date)

        payload["distillate"].append(round(gal, 4))
        payload["gulf_distillate"].append(round(g, 4) if g is not None else None)
        payload["gasoline"].append(round(gas, 4) if gas is not None else None)
        payload["wti"].append(w)
        payload["brent"].append(b)
        payload["us"].append(crack(dist_bbl, w))
        payload["eu"].append(crack(dist_bbl, b))
        payload["gulf"].append(crack(g * GAL_PER_BBL, w) if g is not None else None)
        # 3-2-1: three barrels of crude in, two gasoline and one distillate out.
        if gas is not None and w is not None:
            payload["c321"].append(round((2 * gas * GAL_PER_BBL + dist_bbl - 3 * w) / 3, 3))
        else:
            payload["c321"].append(None)

    payload["monthly"] = fetch_monthly()
    last_release, next_release = fetch_release_dates()
    payload["release"] = {"last": last_release, "next": next_release}
    return payload


def _compare(records, kind_prefix, periods_old, arr_old, periods_new, arr_new, key, label):
    """Append one series' restatements to `records`.

    Only periods present in BOTH snapshots can be restated. A period that is
    new to this snapshot is a fresh publication, not a revision.
    """
    if not arr_old or not arr_new:
        return
    old_at = {p: i for i, p in enumerate(periods_old)}
    for i, period in enumerate(periods_new):
        j = old_at.get(period)
        if j is None or j >= len(arr_old) or i >= len(arr_new):
            continue
        before, after = arr_old[j], arr_new[i]
        if before is None and after is None:
            continue
        if before is None:
            kind = "filled"       # published late
        elif after is None:
            kind = "withdrawn"    # pulled back
        elif abs(before - after) <= EPSILON:
            continue
        else:
            kind = "revised"
        records.append({
            "frequency": kind_prefix,
            "series": key,
            "label": label,
            "period": period,
            "before": before,
            "after": after,
            "kind": kind,
        })


def diff_payloads(old, new):
    """Restatements between two snapshots, newest-period first.

    Returns nothing when the snapshots come from different upstreams: EIA and
    the FRED fallback carry slightly different observation counts, so diffing
    across a source switch would report hundreds of phantom revisions.
    """
    if not old or not new:
        return []
    if old.get("source") != new.get("source"):
        print("skipping revision diff: source changed %s -> %s" % (old.get("source"), new.get("source")), flush=True)
        return []

    records = []
    for key, label in DAILY_TRACKED:
        _compare(records, "daily", old.get("dates", []), old.get(key),
                 new.get("dates", []), new.get(key), key, label)

    om, nm = old.get("monthly"), new.get("monthly")
    if om and nm:
        for key, label in MONTHLY_TRACKED:
            _compare(records, "monthly", om.get("periods", []), om.get(key),
                     nm.get("periods", []), nm.get(key), key, label)

    records.sort(key=lambda r: (r["period"], r["series"]), reverse=True)
    return records


def record_revisions(records, observed_at):
    """Append to the log and return (recent_records, total_count)."""
    if records:
        with open(REVISIONS, "a") as fh:
            for rec in records:
                rec = dict(rec, observed_at=observed_at)
                fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
        try:
            os.chmod(REVISIONS, 0o644)
        except OSError:
            pass
    return read_revisions(REVISIONS_IN_PAYLOAD)


def read_revisions(limit):
    """Return (most recent `limit` records, total). Newest first."""
    if not os.path.exists(REVISIONS):
        return [], 0
    out = []
    with open(REVISIONS) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except ValueError:
                continue
    return list(reversed(out[-limit:])), len(out)


def tracking_since(default_iso):
    """First moment two snapshots could be compared. Written once, then read."""
    if os.path.exists(REVISIONS_META):
        try:
            with open(REVISIONS_META) as fh:
                return json.load(fh).get("since", default_iso)
        except (ValueError, OSError):
            pass
    try:
        with open(REVISIONS_META, "w") as fh:
            json.dump({"since": default_iso}, fh)
        os.chmod(REVISIONS_META, 0o644)
    except OSError:
        pass
    return default_iso


def refresh():
    try:
        payload = build_payload()
    except Exception as exc:  # noqa: BLE001 - any failure falls back to cache
        with _lock:
            _state["last_error"] = "%s: %s" % (type(exc).__name__, exc)
        print("refresh failed: %s" % _state["last_error"], flush=True)
        return False

    # Diff against the snapshot this one replaces, before it is replaced.
    with _lock:
        previous = _state["payload"]
    revisions = diff_payloads(previous, payload)
    if revisions:
        kinds = {}
        for r in revisions:
            kinds[r["kind"]] = kinds.get(r["kind"], 0) + 1
        print("restatements detected: %d (%s)" % (len(revisions), kinds), flush=True)
    recent, total = record_revisions(revisions, payload["updated"])
    payload["revisions"] = {
        "since": tracking_since(payload["updated"]),
        "total": total,
        "last_run": len(revisions),
        "recent": recent,
    }

    with _lock:
        _state["payload"] = payload
        _state["fetched_at"] = time.time()
        _state["last_error"] = None
    tmp = CACHE + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(payload, fh, separators=(",", ":"))
    os.chmod(tmp, 0o644)
    os.replace(tmp, CACHE)
    print("refreshed: source=%s points=%d latest=%s" % (payload["source"], len(payload["dates"]), payload["dates"][-1]), flush=True)
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


def seconds_until_next_slot(now=None):
    """Seconds until the soonest REFRESH_AT_UTC time of day."""
    now = now or datetime.now(timezone.utc)
    soonest = None
    for slot in REFRESH_AT_UTC:
        hour, minute = (int(part) for part in slot.split(":"))
        moment = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if moment <= now:
            moment += timedelta(days=1)
        if soonest is None or moment < soonest:
            soonest = moment
    return max(1.0, (soonest - now).total_seconds())


def next_delay(failures):
    """Seconds to wait before the next attempt.

    A clean run sleeps to the next scheduled slot. After a failure the wait
    starts at a minute and doubles, capped at RETRY_MAX_SECONDS -- a transient
    EIA blip costs a minute rather than waiting out the next slot.
    """
    if failures <= 0:
        return seconds_until_next_slot()
    doublings = min(failures - 1, RETRY_MAX_DOUBLINGS)
    return min(RETRY_BASE_SECONDS * (2 ** doublings), RETRY_MAX_SECONDS)


def refresher(failures=0):
    while True:
        delay = next_delay(failures)
        with _lock:
            _state["failures"] = failures
            _state["next_attempt"] = time.time() + delay
        if failures:
            print("retry %d in %ss" % (failures, round(delay)), flush=True)
        time.sleep(delay)
        failures = 0 if refresh() else failures + 1


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "diesel-api"

    def _send(self, code, body, ctype="application/json"):
        blob = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(blob)))
        # Short, and the page revalidates: a long TTL here means a viewer can
        # be served a payload from before the last refresh — including one with
        # a different shape right after a deploy.
        self.send_header("Cache-Control", "public, max-age=300, must-revalidate")
        self.end_headers()
        self.wfile.write(blob)

    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler API
        path = self.path.split("?")[0]
        with _lock:
            payload = _state["payload"]
            fetched_at = _state["fetched_at"]
            last_error = _state["last_error"]
            failures = _state["failures"]
            next_attempt = _state["next_attempt"]

        if path == "/api/health":
            # Probe the dependency, not the process: no data means unhealthy.
            if payload is None:
                self._send(503, {"status": "no data", "error": last_error})
            else:
                self._send(200, {
                    "status": "ok",
                    "source": payload.get("source"),
                    "points": len(payload["dates"]),
                    "latest": payload["dates"][-1],
                    "monthly_points": len(payload["monthly"]["periods"]) if payload.get("monthly") else 0,
                    "fetched_at": fetched_at,
                    "revisions_total": payload.get("revisions", {}).get("total", 0),
                    "revisions_last_run": payload.get("revisions", {}).get("last_run", 0),
                    "release": payload.get("release"),
                    "consecutive_failures": failures,
                    "next_attempt_in": round(next_attempt - time.time()) if next_attempt else None,
                    "last_error": last_error,
                })
            return

        if path == "/api/revisions":
            qs = urllib.parse.parse_qs(self.path.partition("?")[2])
            try:
                limit = max(1, min(5000, int(qs.get("limit", ["500"])[0])))
            except ValueError:
                limit = 500
            recent, total = read_revisions(limit)
            self._send(200, {
                "since": payload["revisions"]["since"] if payload and payload.get("revisions") else None,
                "total": total,
                "returned": len(recent),
                "revisions": recent,
            })
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
    print("starting; EIA key %s" % ("present" if EIA_KEY else "ABSENT (FRED fallback only)"), flush=True)
    load_cache()
    ok = refresh()
    # A failed first fetch enters the backoff schedule immediately rather than
    # sleeping out the full interval before its first retry.
    threading.Thread(target=refresher, kwargs={"failures": 0 if ok else 1}, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()


if __name__ == "__main__":
    main()
