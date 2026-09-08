# Diesel Crack Spread

Forty years of the refiner's margin on the diesel leg: what a barrel's worth of NY Harbor
distillate fetches over the crude it was made from, US against WTI and Atlantic-basin against
Brent. Live at [diesel.chrislawrence.ca](https://diesel.chrislawrence.ca).

No framework, no build step, no package manager. Plain HTML, CSS and vanilla JS with `fetch`.
It has to still work in three years when nobody has run an install in as long.

## Layout

```
src/index.html      markup, styling and every render function
api/server.py       fetcher, revision tracking and the read-only API
api/econcore.py     vendored from econ-core -- do not edit here
data/recessions.json vendored from econ-core -- the shared band dataset
data/crack.json     the page payload, machine-owned, rewritten each refresh
data/series.json    the same numbers in contract shape, machine-owned
tools/eia-watch.py  observes when EIA actually publishes
```

## Sources

EIA API v2 is primary: it is the original publisher, and it is the only route to the Gulf Coast
leg, the 3-2-1 crack and the monthly panel. FRED's keyless CSV endpoint is the documented
fallback, redistributing the same EIA spot series, so a fallback run produces identical numbers
for the four core legs and simply loses the rest. The header says which source produced what
you are looking at.

**The FRED fallback must go out under urllib's default User-Agent.** `fredgraph.csv` tarpits
requests whose UA it does not recognise as a known tool: curl, wget and Python-urllib get an
answer in a fraction of a second, while a custom or browser-imitating UA from a non-browser TLS
stack hangs until timeout. This app shipped a custom UA on that path and the fallback was dead
for months without anyone noticing, because the EIA key was set and the fallback never ran. The
fetch now goes through `econcore.fred_series`, which is where that policy lives for the whole
collection.

## Two shapes, one refresh

`crack.json` is what the page reads: columnar, every series aligned on one date axis, cheap to
render. `series.json` is the same numbers in the [econ-core](https://github.com/Lawrence908/econ-core) contract shape, served
at `/api/series` and never read by the page.

The contract view exists so the overlay site can put a $/bbl crack on common axes with an
unemployment rate from [jobs](https://github.com/Lawrence908/jobs) or a debt ratio from [debt](https://github.com/Lawrence908/debt) without any of those
pages being rewritten first. Twelve series are published: four cracks, five spot legs, three
monthly refiner series. It lives in its own file rather than in the page payload because contract
`obs` are `[date, value]` pairs, and ten thousand daily points across nine series is a couple of
megabytes the page would fetch and ignore.

Cracks carry `confidence: estimate`, not `reported`. Neither EIA nor FRED publishes a spread, so
the arithmetic is ours; the raw legs it is computed from are published beside it so a reader can
check it rather than trust it.

## The splice

The daily distillate leg has one seam: NY Harbor ULSD from 2006-06-14, No. 2 heating oil before
it. Same barge market, different sulfur spec, so pre-2006 levels are marginally understated. That
splice is what buys the history -- daily ULSD alone only reaches 2006.

It is stated in the payload as a `splices` array rather than a bare date, which is the shape the
contract uses, and it is carried by every series computed from that leg. The monthly panel is a
genuinely different price basis (refiner survey averages, not spot markers) and is therefore
published as its own series and never spliced onto the daily ones.

## Recession bands

Shaded from `data/recessions.json`, vendored from econ-core: US bands derived mechanically from
NBER dates via FRED `USREC`, peak month through trough month inclusive. Same dataset every
tracker on this site shades from, so a band here lines up with the same band on jobs.

Read at serve time rather than baked into the payload, so a re-vendor reaches the page on the
next request instead of waiting out the next refresh. Only the US chronology is drawn; every
series here is a US or Atlantic-basin oil price, so the Canadian bands in the same file have
nothing to date.

Refining margins are not a recession indicator and the page does not offer them as one.

## Revisions

Every refresh is diffed against the one it replaces, and any change to an already-published value
is appended to `data/revisions.jsonl` through `econcore.log_revision`, so a diesel revision reads
the same as one from jobs or debt. Spot prices are rarely restated; the monthly refiner series are
revised routinely as survey responses arrive.

New observations are not revisions and are not logged. Comparison is skipped entirely when the
upstream source changes between snapshots, since EIA and FRED carry slightly different observation
counts and diffing across a switch would report hundreds of phantom restatements.

## Endpoints

| Route | Serves |
|---|---|
| `/api/data` | the page payload, plus the recession bands |
| `/api/series` | the econ-core contract view -- what the overlay site reads |
| `/api/recessions` | the shared band dataset on its own |
| `/api/revisions` | the restatement log, `?limit=` up to 5000 |
| `/api/health` | source, freshness, econcore version, failure state |

## Re-vendoring econ-core

```bash
cd ../econ-core && ./vendor.sh ../diesel
```

Rewrites `api/econcore.py` and `data/recessions.json` with a stamp naming the commit they came
from. Apps carry copies rather than importing a shared path, so nothing rots when no install has
run in three years. Re-vendor deliberately, per app.

## Deploy

```bash
./deploy.sh
```

Needs `EIA_API_KEY` in `.env`. `FRED_API_KEY` is optional and only routes the fallback through
FRED's JSON API instead of the keyless CSV endpoint.

## Data and attribution

The MIT licence covers this repository's code. It does not cover the data, which is not
mine: every series belongs to the body that publishes it and carries that body's own terms.
Each series names its `source` and `source_url` so the original is always one click away.

Prices come from the [US Energy Information Administration](https://www.eia.gov/),
whose data is a work of the US government and not subject to copyright. The EIA asks
that it be cited as the source.

Recession bands come from econ-core: the US from the NBER chronology via FRED `USREC`,
Canada from the C.D. Howe Institute Business Cycle Council chronology.

Series reached through FRED are redistributed by the Federal Reserve Bank of St. Louis
under [its terms of use](https://fred.stlouisfed.org/legal/), which ask that you cite the
original source and note that it was accessed via FRED.
