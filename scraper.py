#!/usr/bin/env python3
"""Resumable BrasilAPI collector. Python 3.10+, no third-party dependencies."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

STAGES = ("eleicoes", "cnpj", "fipe", "fundos_cvm")
RETRYABLE = {408, 425, 429, 500, 502, 503, 504}


def now():
    return datetime.now(timezone.utc).isoformat()


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(encode(value).encode()).hexdigest()


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temp.replace(path)


def cnpj(value):
    """Normalize numeric and new alphanumeric CNPJs; never coerce/mend CPF IDs."""
    if not isinstance(value, str):
        raise ValueError("CNPJ must be a string (leading zeros matter)")
    value = re.sub(r"[./\-\s]", "", value.upper())
    if not re.fullmatch(r"[0-9A-Z]{12}[0-9]{2}", value):
        raise ValueError("CNPJ must contain 12 alphanumeric characters and 2 digits")
    return value


def harvest(value):
    """Only explicitly named CNPJ fields, not arbitrary 14-character strings."""
    found = set()
    if isinstance(value, dict):
        for key, item in value.items():
            if "cnpj" in key.lower():
                try:
                    found.add(cnpj(item))
                except ValueError:
                    pass
            found.update(harvest(item))
    elif isinstance(value, list):
        for item in value:
            found.update(harvest(item))
    return found


def component(value):
    return urllib.parse.quote(str(value), safe="")


def items(value, *wrappers):
    if isinstance(value, dict):
        for name in wrappers:
            if name in value:
                value = value[name]
                break
    if not isinstance(value, list):
        raise ValueError("Expected an array" + (f" or {wrappers}" if wrappers else ""))
    return value


def field(value, *names):
    if not isinstance(value, dict):
        raise ValueError("Expected an object")
    for name in names:
        if value.get(name) is not None:
            return str(value[name])
    raise ValueError("Missing field: " + "/".join(names))


def retry_delay(header, attempt, clock=time.time):
    if header:
        try:
            delay = float(header)
        except ValueError:
            try:
                delay = parsedate_to_datetime(header).timestamp() - clock()
            except (ValueError, TypeError, OverflowError):
                delay = 2 ** attempt
        if math.isfinite(delay):
            return max(0, delay)
    return min(60, 2 ** attempt)


class StageStop(Exception):
    pass


class Store:
    """Success-only cache: failed requests are retried on subsequent runs."""

    def __init__(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS responses (
                url TEXT PRIMARY KEY, stage TEXT NOT NULL, fetched_at TEXT NOT NULL,
                body TEXT NOT NULL, sha256 TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS visits (
                run TEXT NOT NULL, url TEXT NOT NULL, stage TEXT NOT NULL,
                PRIMARY KEY(run, url)
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY, run TEXT NOT NULL, stage TEXT NOT NULL,
                body TEXT NOT NULL
            );
        """)

    def get(self, url):
        row = self.db.execute("SELECT body FROM responses WHERE url=?", (url,)).fetchone()
        return json.loads(row[0]) if row else None

    def put(self, url, stage, data):
        body = encode(data)
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO responses VALUES (?,?,?,?,?)",
                            (url, stage, now(), body, hashlib.sha256(body.encode()).hexdigest()))

    def visit(self, run, url, stage):
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO visits VALUES (?,?,?)", (run, url, stage))

    def event(self, run, stage, value):
        with self.db:
            self.db.execute("INSERT INTO events(run,stage,body) VALUES (?,?,?)",
                            (run, stage, encode(value)))

    def close(self):
        self.db.close()


class Client:
    def __init__(self, store, args, run_id, opener=urllib.request.urlopen,
                 sleep=time.sleep, clock=time.monotonic):
        self.store, self.args, self.run_id = store, args, run_id
        self.opener, self.sleep, self.clock = opener, sleep, clock
        self.next_request = 0.0
        self.stage = None
        self.stats = None
        self.seen = set()
        self.consecutive_failures = 0
        self.bytes_saved = 0
        self.halt_reason = None

    def begin(self, stage, stats):
        self.stage, self.stats = stage, stats
        self.seen = set()
        self.consecutive_failures = 0

    def note(self, kind, **details):
        event = {"time": now(), "kind": kind, **details}
        self.store.event(self.run_id, self.stage, event)

    def get(self, path, **params):
        query = urllib.parse.urlencode(sorted(params.items()))
        url = self.args.base_url.rstrip("/") + path + ("?" + query if query else "")
        # A duplicate failure should not multiply attempts within a run.
        if url in self.seen:
            return self.store.get(url)
        cached = self.store.get(url)
        if cached is not None:
            self.seen.add(url)
            self.store.visit(self.run_id, url, self.stage)
            self.stats["cached"] += 1
            return cached
        if self.halt_reason:
            raise StageStop(self.halt_reason)
        self.seen.add(url)
        for attempt in range(self.args.retries + 1):
            if self.args.max_requests and self.stats["attempts"] >= self.args.max_requests:
                raise StageStop("request_budget")
            self.sleep(max(0, self.next_request - self.clock()))
            self.next_request = self.clock() + self.args.delay
            self.stats["attempts"] += 1
            request = urllib.request.Request(url, headers={
                "User-Agent": "brasilapidumped/1.0 (sequential archival collector)",
                "Accept": "application/json",
            })
            status, wait_header, error, kind = None, None, None, None
            try:
                with self.opener(request, timeout=self.args.timeout) as response:
                    status = response.status
                    if status != 200:
                        raise ValueError(f"Expected HTTP 200, received {status}")
                    body = response.read(self.args.max_response_bytes + 1)
                    if len(body) > self.args.max_response_bytes:
                        raise ValueError("Response exceeds max-response-bytes")
                    data = json.loads(body)
                    if not isinstance(data, (dict, list)):
                        raise ValueError("Expected a JSON object or array")
                size = len(encode(data).encode())
                if self.bytes_saved + size > self.args.max_data_bytes:
                    self.note("storage_budget", url=url, response_bytes=size)
                    self.halt_reason = "storage_budget"
                    raise StageStop(self.halt_reason)
                self.store.put(url, self.stage, data)
                self.store.visit(self.run_id, url, self.stage)
                self.bytes_saved += size
                self.stats["fetched"] += 1
                self.consecutive_failures = 0
                return data
            except urllib.error.HTTPError as exc:
                status = exc.code
                wait_header = exc.headers.get("Retry-After")
                # Error bodies are diagnostics, never exported as data.
                error = exc.read(2048).decode("utf-8", errors="replace")
                exc.close()
                kind = "http"
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                error, kind = str(exc), "transport"
            except (ValueError, UnicodeError) as exc:
                error, kind = str(exc), "invalid_response"
            wait = retry_delay(wait_header, attempt)
            retry = ((kind == "transport" or status in RETRYABLE)
                     and attempt < self.args.retries and wait <= self.args.max_retry_after)
            self.note(kind, url=url, attempt=attempt + 1, status=status,
                      error=error, retrying=retry, retry_after_seconds=wait if retry else None)
            if retry:
                self.sleep(wait)
                continue
            self.stats["failed"] += 1
            if status == 429 or (wait_header and wait > self.args.max_retry_after):
                # A long server-directed pause applies to the service, not just this URL.
                self.halt_reason = "server_retry_later"
                raise StageStop(self.halt_reason)
            if wait_header:
                self.sleep(wait)
            self.consecutive_failures += 1
            if self.consecutive_failures >= self.args.failure_limit:
                raise StageStop("consecutive_failures")
            return None
        return None


class Scraper:
    def __init__(self, client, args):
        self.client, self.args = client, args
        self.seeds = {}  # CNPJ -> sorted provenance strings in the exported seed set.
        self.deferred = {}
        self.contexts = []
        self.municipalities = []
        self.report = {"schema_version": 1, "run_id": client.run_id,
                       "collector_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                       "python_version": sys.version.split()[0],
                       "started_at": now(), "base_url": args.base_url,
                       "order": list(STAGES), "stages": {},
                       "settings": {k: v for k, v in vars(args).items() if k not in
                                    {"output", "state", "cnpj_file", "election_scopes", "municipalities"}}}
        self.report["limitations"] = [
            "CNPJ is seed-bounded; BrasilAPI has no CNPJ enumeration endpoint.",
            "Election detail coverage requires TSE municipalities/scopes; IBGE codes are not substituted.",
            "CVM-discovered CNPJs are deferred; no CNPJ requests occur after the CNPJ stage.",
            "Resume reuses successful responses. Use a new --state for a fresh collection.",
            "Responses collected at different times are not a transactional snapshot.",
            "FIPE upstream fallback may ignore historical reference tables; verify mesReferencia.",
        ]

    def load_inputs(self):
        if self.args.cnpj_file:
            path = Path(self.args.cnpj_file)
            for line in path.read_text(encoding="utf-8-sig").splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    self.seeds.setdefault(cnpj(line), set()).add("input_file")
        if self.args.municipalities:
            for line in Path(self.args.municipalities).read_text(encoding="utf-8-sig").splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    if not re.fullmatch(r"[0-9A-Z]+", line):
                        raise ValueError("Invalid TSE municipality/electoral-unit code")
                    self.municipalities.append(line)
            self.municipalities = sorted(set(self.municipalities))
        if self.args.election_scopes:
            data = json.loads(Path(self.args.election_scopes).read_text(encoding="utf-8"))
            for row in items(data):
                ctx = {k: field(row, k) for k in ("election", "year", "municipality")}
                if not re.fullmatch(r"\d{4}", ctx["year"]):
                    raise ValueError("Election year must contain four digits")
                if not all(re.fullmatch(r"[0-9A-Z]+", value) for value in ctx.values()):
                    raise ValueError("Invalid election scope identifier")
                self.contexts.append(ctx)
        self.report["inputs"] = {"election_scopes": self.contexts,
                                 "tse_municipalities": self.municipalities}

    def remember(self, data, destination, provenance):
        for value in harvest(data):
            destination.setdefault(value, set()).add(provenance)

    def issue(self, reason):
        self.current["limitations"].append(reason)

    def eleicoes(self):
        self.client.get("/eleicoes/anos-eleitorais")
        ordinary = self.client.get("/eleicoes/ordinarias")
        contexts = list(self.contexts)
        if self.municipalities and ordinary is not None:
            for election in items(ordinary, "eleicoes"):
                for municipality in self.municipalities:
                    contexts.append({"election": field(election, "id"),
                                     "year": field(election, "ano"), "municipality": municipality})
        if not contexts:
            self.issue("Election detail traversal blocked: no TSE municipality/election scopes available.")
        self.current["scope"] = "catalogs_and_supplied_electoral_units"
        seen = set()
        for ctx in contexts:
            key = encode(ctx)
            if key in seen:
                continue
            seen.add(key)
            positions = self.client.get("/eleicoes/cargos-por-municipio",
                                        election=ctx["election"], municipality=ctx["municipality"])
            if positions is None:
                continue
            for position in items(positions, "cargos"):
                candidates = self.client.get("/eleicoes/candidaturas", **ctx,
                                             position=field(position, "codigo", "id"))
                if candidates is None:
                    continue
                self.remember(candidates, self.seeds, "eleicoes")
                for candidate in items(candidates, "candidatos"):
                    detail = self.client.get("/eleicoes/candidaturas/" + component(field(candidate, "id")), **ctx)
                    if detail is not None:
                        self.remember(detail, self.seeds, "eleicoes")

    def cnpj(self):
        self.current["scope"] = "frozen_seed_set_only"
        self.current["seed_count"] = len(self.seeds)
        self.current["seed_sha256"] = digest(sorted(self.seeds))
        self.report["cnpj_seeds"] = {k: sorted(v) for k, v in sorted(self.seeds.items())}
        if not self.seeds:
            self.issue("CNPJ lookup blocked: no input/discovered CNPJs; no identifiers guessed.")
        # Intentionally freeze: neither corporate links nor later CVM results expand this stage.
        for value in sorted(self.seeds):
            self.client.get("/cnpj/v1/" + component(value))

    def fipe(self):
        self.current["scope"] = "all_reference_tables" if self.args.fipe_tables == "all" else "latest_reference_table"
        tables = self.client.get("/fipe/tabelas/v1")
        if tables is None:
            return
        codes = sorted({int(field(t, "codigo")) for t in items(tables)}, reverse=True)
        if not codes:
            self.issue("No FIPE reference tables returned; traversal unavailable.")
        if self.args.fipe_tables == "latest":
            codes = codes[:1]
        self.current["reference_tables"] = codes
        for table in codes:
            query = {"tabela_referencia": table}
            # Combined brands contain no type discriminator, so use typed catalogs for traversal.
            self.client.get("/fipe/marcas/v1", **query)
            for vehicle in ("carros", "motos", "caminhoes"):
                brands = self.client.get("/fipe/marcas/v1/" + vehicle, **query)
                if brands is None:
                    continue
                for brand in items(brands):
                    prefix = vehicle + "/" + component(field(brand, "valor"))
                    models = self.client.get("/fipe/veiculos/v1/" + prefix, **query)
                    if models is None:
                        continue
                    for model in items(models):
                        model_path = prefix + "/" + component(field(model, "valor"))
                        years = self.client.get("/fipe/anos/v1/" + model_path, **query)
                        if years is None:
                            continue
                        for year in items(years):
                            detail = self.client.get("/fipe/detalhes/v1/" + model_path + "/" +
                                                     component(field(year, "valor")), **query)
                            if detail is not None:
                                # URL cache deduplicates prices across years of the same FIPE code/table.
                                code = field(detail, "codigoFipe")
                                self.client.get("/fipe/preco/v1/" + component(code), **query)

    def fundos_cvm(self):
        self.current["scope"] = "all_pages_and_unique_fund_details"
        page, seen_pages, seen_funds = 1, set(), set()
        self.current["pagination_exhausted"] = False
        while True:
            result = self.client.get("/cvm/fundos/v1", page=page, size=self.args.page_size)
            if result is None:
                self.issue(f"Pagination blocked at page {page}; subsequent pages not guessed.")
                break
            funds = items(result, "data")
            if not isinstance(result, dict) or result.get("page") != page or result.get("size") != self.args.page_size:
                raise ValueError("CVM pagination metadata does not match requested page/size")
            if len(funds) > self.args.page_size:
                raise ValueError("CVM page exceeds requested size")
            signature = digest(funds)
            if funds and signature in seen_pages:
                raise ValueError("Repeated CVM page; refusing an infinite pagination loop")
            seen_pages.add(signature)
            self.remember(funds, self.deferred, "fundos_cvm")
            for fund in funds:
                value = cnpj(field(fund, "cnpj"))
                if value in seen_funds:
                    continue
                seen_funds.add(value)
                detail = self.client.get("/cvm/fundos/v1/" + component(value))
                if detail is not None:
                    self.remember(detail, self.deferred, "fundos_cvm")
            self.current["pages"] = page
            self.current["unique_funds"] = len(seen_funds)
            if len(funds) < self.args.page_size:
                self.current["pagination_exhausted"] = True
                break
            page += 1

    def run(self):
        interrupted = False
        for stage in STAGES:
            self.current = {"started_at": now(), "status": "running", "attempts": 0,
                            "fetched": 0, "cached": 0, "failed": 0, "limitations": []}
            self.report["stages"][stage] = self.current
            self.client.begin(stage, self.current)
            print(f"[{stage}] starting", flush=True)
            try:
                getattr(self, stage)()
            except StageStop as exc:
                self.current["stop_reason"] = str(exc)
            except (ValueError, KeyError, TypeError) as exc:
                self.client.note("schema_error", error=str(exc))
                self.current["stop_reason"] = "schema_error"
                self.issue(str(exc))
            except KeyboardInterrupt:
                interrupted = True
                self.current["stop_reason"] = "interrupted"
            successful = self.current["fetched"] + self.current["cached"]
            if self.current.get("stop_reason") or self.current["failed"]:
                self.current["status"] = "partial" if successful else "failed"
            elif self.current["limitations"]:
                self.current["status"] = "partial" if successful else "blocked"
            else:
                self.current["status"] = "complete_for_scope"
            self.current["finished_at"] = now()
            print(f"[{stage}] {self.current['status']}: {successful} responses, "
                  f"{self.current['failed']} failed URLs", flush=True)
            # Durable per-stage report even if a later stage is interrupted.
            atomic_json(Path(self.args.output) / "manifest.json", self.report)
            if interrupted:
                break
        for stage in STAGES:
            self.report["stages"].setdefault(stage, {"status": "not_started"})
        self.report["deferred_cnpj_count"] = len(self.deferred)
        self.report["finished_at"] = now()
        self.report["status"] = "complete_for_scope" if all(
            s["status"] == "complete_for_scope" for s in self.report["stages"].values()) else "incomplete"
        return 130 if interrupted else (0 if self.report["status"] == "complete_for_scope" else 2)

    def export(self):
        """Bounded, streaming exports; SQLite remains the complete local checkpoint."""
        output = Path(self.args.output)
        output.mkdir(parents=True, exist_ok=True)
        files, remaining = [], self.args.export_max_bytes
        for stage in STAGES:
            path = output / f"{stage}.jsonl.gz"
            # Output directory belongs to this run; remove only our known prior exports.
            path.unlink(missing_ok=True)
            rows = self.client.store.db.execute("""
                SELECT r.url,r.fetched_at,r.body,r.sha256 FROM responses r JOIN visits v
                ON r.url=v.url WHERE v.run=? AND v.stage=? ORDER BY r.url
            """, (self.client.run_id, stage))
            count, truncated = 0, False
            temp = path.with_name(path.name + ".tmp")
            with temp.open("wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, filename="") as zipped:
                for url, fetched_at, body, sha in rows:
                    line = (encode({"url": url, "fetched_at": fetched_at, "sha256": sha,
                                    "data": json.loads(body)}) + "\n").encode()
                    if len(line) > remaining:
                        truncated = True
                        break
                    zipped.write(line)
                    remaining -= len(line)
                    count += 1
            if count:
                temp.replace(path)
                files.append({"path": path.name, "responses": count,
                              "bytes": path.stat().st_size,
                              "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
            else:
                temp.unlink()
            self.report["stages"][stage]["exported_responses"] = count
            if truncated:
                self.report["stages"][stage]["export_truncated"] = True
                self.report["status"] = "incomplete"
        errors_path = output / "errors.jsonl"
        errors, errors_truncated, error_bytes = 0, False, 0
        with errors_path.open("w", encoding="utf-8") as stream:
            for stage, body in self.client.store.db.execute(
                    "SELECT stage,body FROM events WHERE run=? ORDER BY id", (self.client.run_id,)):
                line = encode({"stage": stage, **json.loads(body)}) + "\n"
                error_bytes += len(line.encode())
                if error_bytes > 2 * 1024 * 1024:
                    errors_truncated = True
                    break
                stream.write(line)
                errors += 1
        self.report["error_export"] = {"path": errors_path.name, "events": errors,
                                       "truncated": errors_truncated}
        seeds_path = output / "deferred_cnpjs.txt"
        seeds_path.write_text("".join(value + "\n" for value in sorted(self.deferred)), encoding="utf-8")
        self.report["exports"] = files
        self.report["deferred_cnpj_sha256"] = digest(sorted(self.deferred))
        atomic_json(output / "manifest.json", self.report)


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--base-url", default="https://brasilapi.com.br/api")
    p.add_argument("--output", default="output")
    p.add_argument("--state", default=".cache/scraper.sqlite3")
    p.add_argument("--cnpj-file", help="One CNPJ per line; e.g. a prior run's deferred_cnpjs.txt")
    p.add_argument("--election-scopes", help="JSON array of {election, year, municipality} objects")
    p.add_argument("--municipalities", help="One TSE electoral-unit code per line, applied to all ordinary elections")
    p.add_argument("--fipe-tables", choices=("all", "latest"), default="all")
    p.add_argument("--delay", type=float, default=1.0, help="Minimum seconds between requests")
    p.add_argument("--timeout", type=float, default=30.0)
    p.add_argument("--retries", type=int, default=2, help="Retries after the initial attempt")
    p.add_argument("--max-retry-after", type=float, default=120.0,
                   help="If Retry-After exceeds this, fail without retrying early")
    p.add_argument("--failure-limit", type=int, default=5, help="Consecutive failures before pausing a stage")
    p.add_argument("--max-requests", type=int, default=0, help="HTTP attempts per stage; 0 means unlimited")
    p.add_argument("--page-size", type=int, default=200)
    p.add_argument("--max-response-bytes", type=int, default=16 * 1024 * 1024)
    p.add_argument("--max-data-bytes", type=int, default=96 * 1024 * 1024,
                   help="Uncompressed successful data added to cache per run")
    p.add_argument("--export-max-bytes", type=int, default=48 * 1024 * 1024,
                   help="Total uncompressed JSONL data export limit (does not limit cache)")
    return p


def main(argv=None):
    p = parser()
    args = p.parse_args(argv)
    for name in ("delay", "timeout", "max_retry_after"):
        if not math.isfinite(getattr(args, name)) or getattr(args, name) < 0:
            p.error(f"--{name.replace('_', '-')} must be finite and nonnegative")
    for name in ("timeout", "failure_limit", "max_response_bytes", "max_data_bytes", "export_max_bytes"):
        if getattr(args, name) <= 0:
            p.error(f"--{name.replace('_', '-')} must be positive")
    if args.retries < 0 or args.max_requests < 0 or not 1 <= args.page_size <= 200:
        p.error("retries/max-requests must be nonnegative; page-size must be 1..200")
    parts = urllib.parse.urlsplit(args.base_url)
    if parts.scheme not in {"http", "https"} or not parts.netloc or parts.query or parts.fragment or parts.username:
        p.error("base-url must be an HTTP(S) API root without credentials, query or fragment")
    store = Store(args.state)
    try:
        collector = Scraper(Client(store, args, now()), args)
        try:
            collector.load_inputs()
        except (OSError, ValueError) as exc:
            p.error(f"Invalid input: {exc}")
        code = collector.run()
        collector.export()
        print(f"Report: {Path(args.output) / 'manifest.json'}", flush=True)
        return code if code else (2 if collector.report["status"] != "complete_for_scope" else 0)
    finally:
        store.close()


if __name__ == "__main__":
    sys.exit(main())
