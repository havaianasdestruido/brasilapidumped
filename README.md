# BrasilAPI dump collector

A dependency-free Python 3.10+ scraper for [BrasilAPI](https://brasilapi.com.br/docs).
It always processes the API groups sequentially:

**Eleições → CNPJ → FIPE → Fundos CVM**

## Committed live run

The run on **2026-09-28** attempted maximum discoverable coverage but collected
**zero successful API responses**. Every HTTP attempt failed during TLS setup
with `TLS/SSL connection has been closed (EOF)` from this workspace. This does
not establish that the API itself is down. CNPJ was blocked because no seed
identifiers were available. No example responses were substituted for live data.

Actual machine-generated output is in:

- [`output/manifest.json`](output/manifest.json): settings, stage order, timestamps,
  counts, scope limitations and incomplete status.
- [`output/errors.jsonl`](output/errors.jsonl): all 12 failed HTTP attempts,
  including URLs and retry decisions.
- [`output/deferred_cnpjs.txt`](output/deferred_cnpjs.txt): empty in this run.

The command executed was:

```sh
python3 -u scraper.py --fipe-tables all --output output --state .cache/live.sqlite3
```

It exited with code **2**, indicating incomplete collection. Successful data
exports will only be created once the API can be reached.

## Run / resume

No dependencies to install:

```sh
python3 scraper.py
python3 scraper.py --help
python3 -m unittest discover -s tests -v
```

Defaults request **all** advertised FIPE reference tables and all CVM pages.
They do not arbitrarily sample brands, models, years, funds or election years.
The run may be very large and take days or longer. All network requests are
sequential, with at least one second between request starts.

Examples with your own seed files:

```sh
python3 scraper.py \
  --cnpj-file /path/to/cnpjs.txt \
  --election-scopes /path/to/election-scopes.json

# Apply each supplied TSE electoral-unit code to all discovered ordinary elections.
python3 scraper.py --municipalities /path/to/tse-municipalities.txt

# Explicitly smaller scope / per-stage request budget:
python3 scraper.py --fipe-tables latest --max-requests 500
```

`cnpjs.txt`: one CNPJ per line. Formatting and lowercase letters are normalized;
leading zeros are preserved. Numeric and alphanumeric CNPJ formats are supported.
This validates shape, not Receita check digits or existence. Blank lines and
`#` comments are ignored. No identifiers are generated or brute-forced.

`election-scopes.json`: a JSON array of objects with string fields `election`,
`year` and `municipality`. Use actual election IDs and TSE electoral-unit codes,
not IBGE municipality codes. `--municipalities` instead accepts one TSE code
per line. With neither file, the two election catalogs are still requested,
but the missing detail scope is explicitly reported.

### Coverage

| Stage | Traversal | Boundary |
| --- | --- | --- |
| Eleições | Years, ordinary elections, positions per supplied electoral unit, candidate lists, candidate details | No municipality-discovery endpoint exists in this endpoint group. Requires scopes or TSE codes for detail traversal. Handles candidate arrays and the TSE `candidatos` wrapper. |
| CNPJ | Lookups for the deduplicated input CNPJs and explicit CNPJ fields discovered during Eleições | No enumeration endpoint. The seed set is frozen at stage start, not recursively expanded. Masked CPFs and arbitrary strings are not treated as CNPJs. |
| FIPE | Tables → combined/typed brands → models → years/fuels → details → prices by FIPE code | Every request is pinned to its table. Price URLs are deduplicated per code/table. `--fipe-tables latest` explicitly selects only the greatest table code. |
| Fundos CVM | Pages starting at 1, size 200 → unique CNPJ details | Ends on a short/empty page. Repeated pages or mismatched metadata stop traversal; failed pages are never silently skipped. |

CVM-discovered CNPJs (including explicit CNPJ fields in fund details) are saved to
`deferred_cnpjs.txt`, **not queried out of order**. A subsequent run can use that
file with `--cnpj-file`. Such a run still starts at Eleições. This cannot produce
a national CNPJ census; supplying a comprehensive identifier list is necessary.

**FIPE history caveat:** the upstream implementation has fallback paths that may
ignore a requested historical table. Responses are preserved as returned, with
request URLs and `mesReferencia`; table coverage alone is not proof that a price
is historically accurate. The scraper does not silently relabel those prices.

### Reliability and scope reporting

- Timeout: 30 seconds; two retries for transport failures and transient HTTP
  statuses (`408`, `425`, `429`, `500`, `502`, `503`, `504`). Other HTTP errors
  are recorded without retrying.
- Exponential backoff and `Retry-After` seconds / HTTP dates. If a server-directed
  wait exceeds `--max-retry-after` (120 seconds), the run stops further network
  requests rather than retrying early. Exhausted HTTP 429 retries do the same.
- Five consecutive failed URLs pause a stage; later stages still run unless a
  service-wide pause or storage limit prevents further requests.
- `--max-requests` caps **HTTP attempts per stage**, including retries; zero means
  unlimited. Cached responses do not consume this budget.
- SQLite stores each successful response immediately. Restarting with the same
  `--state` reuses those responses and retries failed requests. This is resume,
  **not refresh**: use a new state path for a fresh snapshot. Use the same scope
  and seed inputs when resuming. Do not run multiple collectors against the
  same state/output paths concurrently.
- Ctrl-C finalizes the manifest and exports completed responses. An abrupt kill
  preserves committed SQLite responses and the last completed-stage report;
  rerun the command to export them.
- Exit codes: `0` = complete for the documented scope; `2` = incomplete, limited,
  blocked or invalid input; `130` = interrupted. “Complete for scope” never means
  a complete national registry or a transactional point-in-time snapshot.

### Storage and output

By default:

- SQLite checkpoint: `.cache/scraper.sqlite3` (ignored by Git).
- Up to **96 MiB** of new canonical JSON response data per invocation. The cache
  can grow across resumed runs; monitor disk space.
- Each response is limited to **16 MiB**.
- Up to **48 MiB uncompressed JSONL** across the four data exports. Exported
  error events are separately capped at 2 MiB; truncation is always reported.
- Local state retains data omitted by the export cap. An export limit makes the
  overall report incomplete; it never silently claims a complete exported dump.

Successful exports are `output/{eleicoes,cnpj,fipe,fundos_cvm}.jsonl.gz`. Each
line is an envelope with `url`, `fetched_at`, `sha256`, and `data` containing the
unaltered JSON value. Whitespace/key order are canonicalized; these are not
raw HTTP byte captures. Hashes use UTF-8 JSON with sorted keys, compact separators
and `ensure_ascii=False`. Gzip headers are deterministic. The manifest records
exported counts and file checksums. API errors are separate from data.

To inspect an export:

```python
import gzip
import json

with gzip.open("output/fipe.jsonl.gz", "rt", encoding="utf-8") as stream:
    for line in stream:
        record = json.loads(line)
        print(record["url"], record["data"])
```

Exports include only URLs visited in that invocation, including successful cache
hits, not unrelated responses from older scopes. The chosen output directory is
overwritten for the current run; use a new directory to preserve old snapshots.

For large runs, increase the limits deliberately and keep artifacts in ignored
`dumps/` or external storage, not Git. Example:

```sh
python3 scraper.py --output dumps/large --state .cache/large.sqlite3 \
  --max-data-bytes 1073741824 --export-max-bytes 1073741824
```

API responses can contain personal information in candidate/company records.
Review collected data before publishing it; the scraper does not redact fields.
Tests use synthetic responses exclusively and never populate the live output.

## Endpoint contract references

Endpoint paths, query parameters and response traversal were checked against
BrasilAPI's documentation and upstream source at revision
[`0104c49c94017e47560615c8713ba7686ae2e381`](https://github.com/BrasilAPI/BrasilAPI/tree/0104c49c94017e47560615c8713ba7686ae2e381):

- `pages/docs/doc/{eleicoes,cnpj,fipe,fundos}.json`
- `services/eleicoes/{candidaturas,cargos-por-municipio,ordinarias}.js`
- `services/fipe/{vehiclesByMakers,yearsByModel,priceByModelAndYear}.js`
- `services/cvm/fundos.js`

The configured base URL is `https://brasilapi.com.br/api`. `--base-url` exists for
compatible deployments/testing; TLS verification remains enabled. Documentation
and test fixtures are not evidence of live endpoint availability.
