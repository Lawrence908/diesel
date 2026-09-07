# VENDORED: econ-core 273cdef, vendored 2026-09-07. Do not edit here; edit econ-core and re-vendor.
"""econ-core: shared fetchers and series contract for the economic trackers.

One module, vendored into each app by ../vendor.sh rather than imported from a
shared path. Apps must keep working in three years without anyone running an
install, so there is no runtime coupling: each app carries a stamped copy and
the stamp says which commit it came from.

Fetch policy, shared by every app in the collection: the keyless public
endpoint is primary and the keyed API is the documented fallback (or, for
vintages, the only route). A revoked key degrades a run, never fails it.

The series contract lives in CONTRACT.md and schema/series.schema.json.
validate_series() enforces it at write time so a malformed series is caught by
the updater that produced it, not by the page that renders it.
"""

import csv
import io
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

VERSION = "1.0.0"

FRED_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={}"
FRED_API = "https://api.stlouisfed.org/fred/series/observations"
WDS = "https://www150.statcan.gc.ca/t1/wds/rest/"
VALET = "https://www.bankofcanada.ca/valet/observations/{}/json"

UA = {"User-Agent": "chrislawrence.ca economic trackers (econ-core)"}

# fred.stlouisfed.org tarpits requests whose User-Agent it does not recognise
# as a known tool: curl/wget/Python-urllib get an instant response, while a
# custom or browser-imitating UA from a non-browser TLS stack hangs until
# timeout (observed 2026-09-06). So the CSV endpoint gets urllib's true
# default UA, which is both honest and allowed. api.stlouisfed.org (keyed)
# has no such filter.

CONFIDENCE = ("reported", "estimate", "projection")
FREQUENCIES = ("daily", "weekly", "monthly", "quarterly", "annual")


# --------------------------------------------------------------------------
# transport
# --------------------------------------------------------------------------

def _get(url, timeout=90, default_ua=False):
    req = urllib.request.Request(url, headers={} if default_ua else UA)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8")


def _post_json(url, payload, timeout=90):
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, headers={**UA, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


# --------------------------------------------------------------------------
# FRED / ALFRED
# --------------------------------------------------------------------------

def fred_series(series_id, api_key=""):
    """[[iso_date, float], ...] sorted ascending, missing observations dropped.

    Keyed JSON API when a key is given, keyless CSV otherwise or on an empty
    keyed response (an empty keyed response is more likely a bad key than an
    empty series).
    """
    if api_key:
        params = urllib.parse.urlencode({
            "series_id": series_id, "api_key": api_key, "file_type": "json"})
        body = json.loads(_get("%s?%s" % (FRED_API, params)))
        out = []
        for row in body.get("observations", []):
            if row.get("value") in (None, "", "."):
                continue
            try:
                out.append([row["date"], float(row["value"])])
            except (TypeError, ValueError):
                continue
        if out:
            return sorted(out)

    out = []
    reader = csv.reader(io.StringIO(_get(FRED_CSV.format(series_id),
                                         default_ua=True)))
    next(reader)
    for row in reader:
        if len(row) < 2 or row[1].strip() in ("", "."):
            continue
        try:
            out.append([row[0].strip(), float(row[1].strip())])
        except ValueError:
            continue
    if not out:
        raise ValueError("no observations for %s" % series_id)
    return sorted(out)


def alfred_series(series_id, api_key, vintage_date):
    """The series as it stood on vintage_date (ALFRED real-time view).

    Keyed only: the keyless CSV endpoint has no vintage access. This is the
    route for "what did a forecaster actually see in October 2008" charts;
    revised history overstates how obvious every turn was.
    """
    if not api_key:
        raise ValueError("ALFRED vintages require a FRED API key")
    params = urllib.parse.urlencode({
        "series_id": series_id, "api_key": api_key, "file_type": "json",
        "realtime_start": vintage_date, "realtime_end": vintage_date})
    body = json.loads(_get("%s?%s" % (FRED_API, params)))
    out = []
    for row in body.get("observations", []):
        if row.get("value") in (None, "", "."):
            continue
        try:
            out.append([row["date"], float(row["value"])])
        except (TypeError, ValueError):
            continue
    return sorted(out)


# --------------------------------------------------------------------------
# Statistics Canada WDS
# --------------------------------------------------------------------------

def wds_series_info(vector_id):
    """Metadata for one vector, including its English title.

    Always verify a new vector's title against the published table before
    trusting it. A guessed vector produces a plausible wrong number, which is
    worse than an obvious gap.
    """
    rows = _post_json(WDS + "getSeriesInfoFromVector",
                      [{"vectorId": int(vector_id)}])
    obj = rows[0].get("object") or {}
    if rows[0].get("status") != "SUCCESS" or not obj.get("SeriesTitleEn"):
        raise ValueError("vector %s: no series info" % vector_id)
    return obj


def wds_vector(vector_id, latest_n=5000, expect_title=None):
    """[[iso_date, float], ...] for one StatCan vector, sorted ascending.

    latest_n defaults high enough to return the whole history of any monthly
    series. expect_title, when given, is matched (case-insensitive substring)
    against the vector's live English title first; a mismatch raises rather
    than returning someone else's numbers.
    """
    if expect_title:
        title = wds_series_info(vector_id).get("SeriesTitleEn", "")
        if expect_title.lower() not in title.lower():
            raise ValueError("vector %s title %r does not contain %r"
                             % (vector_id, title, expect_title))
    rows = _post_json(WDS + "getDataFromVectorsAndLatestNPeriods",
                      [{"vectorId": int(vector_id), "latestN": int(latest_n)}])
    if rows[0].get("status") != "SUCCESS":
        raise ValueError("vector %s: %s" % (vector_id, rows[0].get("status")))
    out = []
    for point in rows[0]["object"].get("vectorDataPoint", []):
        value = point.get("value")
        if value is None:
            continue
        out.append([point["refPer"], float(value)])
    if not out:
        raise ValueError("vector %s returned no data points" % vector_id)
    return sorted(out)


# --------------------------------------------------------------------------
# Bank of Canada Valet
# --------------------------------------------------------------------------

def valet_series(series_name, start_date=None):
    """[[iso_date, float], ...] for one Valet series, sorted ascending.

    Keyless JSON. Used by the yield tracker; here so every app fetches Valet
    the same way.
    """
    url = VALET.format(urllib.parse.quote(series_name))
    if start_date:
        url += "?start_date=" + start_date
    body = json.loads(_get(url))
    out = []
    for row in body.get("observations", []):
        cell = row.get(series_name) or {}
        value = cell.get("v")
        if value in (None, ""):
            continue
        try:
            out.append([row["d"], float(value)])
        except (TypeError, ValueError):
            continue
    if not out:
        raise ValueError("valet %s returned no observations" % series_name)
    return sorted(out)


# --------------------------------------------------------------------------
# series contract
# --------------------------------------------------------------------------

def make_series(series_id, label, source, source_url, units, freq, obs,
                confidence="reported", note=None, splices=None, vintages=None):
    """Assemble a contract-shaped series dict and validate it."""
    doc = {
        "id": series_id,
        "label": label,
        "source": source,
        "source_url": source_url,
        "units": units,
        "freq": freq,
        "confidence": confidence,
        "as_of": obs[-1][0] if obs else None,
        "obs": obs,
    }
    if note:
        doc["note"] = note
    if splices:
        doc["splices"] = splices
    if vintages:
        doc["vintages"] = vintages
    problems = validate_series(doc)
    if problems:
        raise ValueError("series %s violates contract: %s"
                         % (series_id, "; ".join(problems)))
    return doc


def validate_series(doc):
    """Return a list of contract violations, empty when clean."""
    problems = []
    for field in ("id", "label", "source", "source_url", "units", "freq",
                  "confidence", "as_of", "obs"):
        if not doc.get(field):
            problems.append("missing %s" % field)
    if problems:
        return problems
    if doc["confidence"] not in CONFIDENCE:
        problems.append("confidence %r not in %s" % (doc["confidence"], CONFIDENCE))
    if doc["freq"] not in FREQUENCIES:
        problems.append("freq %r not in %s" % (doc["freq"], FREQUENCIES))
    if not doc["source_url"].startswith("http"):
        problems.append("source_url is not a URL")
    obs = doc["obs"]
    for pair in obs:
        if (not isinstance(pair, (list, tuple)) or len(pair) != 2
                or not isinstance(pair[0], str)
                or not isinstance(pair[1], (int, float))):
            problems.append("obs must be [iso_date, number] pairs")
            break
    if obs != sorted(obs, key=lambda p: p[0]):
        problems.append("obs not sorted by date")
    if obs and doc["as_of"] != obs[-1][0]:
        problems.append("as_of %r is not the last observation date %r"
                        % (doc["as_of"], obs[-1][0]))
    for splice in doc.get("splices", []):
        if not splice.get("at") or not splice.get("note"):
            problems.append("splice entries need at + note")
    return problems


# --------------------------------------------------------------------------
# derived helpers
# --------------------------------------------------------------------------

def annual_average(obs):
    """{year: mean} from [[iso_date, value], ...]."""
    sums, counts = {}, {}
    for date, value in obs:
        year = int(date[:4])
        sums[year] = sums.get(year, 0.0) + value
        counts[year] = counts.get(year, 0) + 1
    return {year: sums[year] / counts[year] for year in sums}


def yoy_percent(obs, periods):
    """Year-over-year percent change; periods = observations per year."""
    out = []
    for i in range(periods, len(obs)):
        prev = obs[i - periods][1]
        if prev:
            out.append([obs[i][0], (obs[i][1] / prev - 1.0) * 100.0])
    return out


# --------------------------------------------------------------------------
# revision log
# --------------------------------------------------------------------------

def log_revision(path, record):
    """Append one jsonl record, stamped observed_at. The diesel revision-log
    concept, shared: nothing is silently overwritten; a changed value leaves
    a line saying what changed and when it was noticed."""
    record = dict(record, observed_at=datetime.now(timezone.utc).isoformat())
    with open(path, "a") as fh:
        fh.write(json.dumps(record, separators=(",", ":")) + "\n")
    try:
        os.chmod(path, 0o644)
    except OSError:
        pass
    return record


def read_revisions(path, limit=100):
    if not os.path.exists(path):
        return [], 0
    out = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except ValueError:
                continue
    return list(reversed(out[-limit:])), len(out)


# --------------------------------------------------------------------------
# recessions
# --------------------------------------------------------------------------

def load_recessions(path):
    """The shared recession-band dataset (see tools/build-recessions.py)."""
    with open(path) as fh:
        return json.load(fh)
