"""All test responses are synthetic; no test fixture is exported to output/."""
import gzip
import hashlib
import io
import json
import tempfile
import unittest
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import scraper


class Response(io.BytesIO):
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class Wire:
    def __init__(self, route):
        self.route, self.calls = route, []

    def __call__(self, request, timeout):
        self.calls.append(request.full_url)
        result = self.route(request.full_url)
        if isinstance(result, Exception):
            raise result
        return Response(json.dumps(result).encode())


def http_error(code, retry_after=None):
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    return urllib.error.HTTPError("https://example.test", code, "test", headers, io.BytesIO(b'{}'))


class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.args = scraper.parser().parse_args([
            "--state", str(self.root / "state.sqlite3"), "--output", str(self.root / "output"),
            "--base-url", "https://example.test/api", "--delay", "0", "--retries", "0",
        ])
        self.store = scraper.Store(self.args.state)
        self.addCleanup(self.store.close)
        self.addCleanup(self.temp.cleanup)

    def collector(self, route, run="test"):
        wire = Wire(route)
        client = scraper.Client(self.store, self.args, run, opener=wire, sleep=lambda _: None)
        return scraper.Scraper(client, self.args), wire

    def begin(self, collector, stage="fipe"):
        stats = {"fetched": 0, "failed": 0, "cached": 0, "attempts": 0}
        collector.client.begin(stage, stats)
        return stats

    def test_numeric_and_alphanumeric_cnpjs(self):
        self.assertEqual(scraper.cnpj("00.000.684/0001-21"), "00000684000121")
        self.assertEqual(scraper.cnpj("ab.cde.fgh/0001-12"), "ABCDEFGH000112")
        for value in ["***112108**", "00000000000", 684000121, None, "@00000684000121"]:
            with self.assertRaises(ValueError):
                scraper.cnpj(value)

    def test_harvest_only_explicit_cnpj_keys(self):
        data = {"cnpj": "00.000.684/0001-21", "cpf": "11111111111111",
                "nested": [{"cnpjEmpresa": "12.345.678/0001-95"}, {"cnpjCpf": "***112108**"}]}
        self.assertEqual(scraper.harvest(data), {"00000684000121", "12345678000195"})

    def test_cache_resumes_and_separates_hosts(self):
        c, wire = self.collector(lambda _: [])
        stats = self.begin(c)
        self.assertEqual(c.client.get("/test", z=1, a=2), [])
        self.assertEqual(c.client.get("/test", a=2, z=1), [])
        self.assertEqual(len(wire.calls), 1)
        self.assertTrue(wire.calls[0].endswith("?a=2&z=1"))
        c2, wire2 = self.collector(lambda _: self.fail("cached URL fetched"), run="second")
        self.begin(c2)
        self.assertEqual(c2.client.get("/test", a=2, z=1), [])
        self.assertEqual(len(wire2.calls), 0)
        self.assertIsNone(self.store.get("https://different.test/api/test?a=2&z=1"))
        self.assertEqual(stats["fetched"], 1)

    def test_failed_urls_not_cached_and_retried_on_resume(self):
        c, wire = self.collector(lambda _: urllib.error.URLError("test network failure"))
        stats = self.begin(c)
        self.assertIsNone(c.client.get("/test"))
        self.assertIsNone(c.client.get("/test"))
        self.assertEqual(len(wire.calls), 1)
        self.assertEqual(stats["failed"], 1)
        c2, wire2 = self.collector(lambda _: {"ok": True}, run="second")
        self.begin(c2)
        self.assertEqual(c2.client.get("/test"), {"ok": True})
        self.assertEqual(len(wire2.calls), 1)

    def test_http_retry_after_and_retry_success(self):
        self.args.retries = 2
        responses = iter([http_error(429, "7"), {"ok": True}])
        c, wire = self.collector(lambda _: next(responses))
        sleeps = []
        c.client.sleep = sleeps.append
        stats = self.begin(c)
        self.assertEqual(c.client.get("/test"), {"ok": True})
        self.assertIn(7, sleeps)
        self.assertEqual(stats["attempts"], 2)
        self.assertEqual(stats["failed"], 0)

    def test_retry_after_formats(self):
        stamp = datetime(2026, 9, 28, tzinfo=timezone.utc).timestamp()
        self.assertEqual(scraper.retry_delay("Mon, 28 Sep 2026 00:01:00 GMT", 0, lambda: stamp), 60)
        self.assertEqual(scraper.retry_delay("bad", 2), 4)
        self.assertEqual(scraper.retry_delay("NaN", 2), 4)
        self.assertEqual(scraper.retry_delay("-1", 2), 0)

    def test_long_retry_after_pauses_all_future_network(self):
        self.args.retries = 2
        c, wire = self.collector(lambda _: http_error(503, "300"))
        self.begin(c)
        with self.assertRaisesRegex(scraper.StageStop, "server_retry_later"):
            c.client.get("/one")
        self.begin(c, "fundos_cvm")
        with self.assertRaisesRegex(scraper.StageStop, "server_retry_later"):
            c.client.get("/two")
        self.assertEqual(len(wire.calls), 1)

    def test_exhausted_rate_limit_pauses_all_future_network(self):
        self.args.retries = 1
        c, wire = self.collector(lambda _: http_error(429))
        self.begin(c)
        with self.assertRaisesRegex(scraper.StageStop, "server_retry_later"):
            c.client.get("/one")
        self.begin(c, "fundos_cvm")
        with self.assertRaisesRegex(scraper.StageStop, "server_retry_later"):
            c.client.get("/two")
        self.assertEqual(len(wire.calls), 2)

    def test_global_storage_pause_does_not_block_cached_data(self):
        self.store.put("https://example.test/api/cached", "fipe", [])
        self.args.max_data_bytes = 1
        c, wire = self.collector(lambda _: {})
        self.begin(c)
        with self.assertRaisesRegex(scraper.StageStop, "storage_budget"):
            c.client.get("/one")
        self.begin(c, "fundos_cvm")
        self.assertEqual(c.client.get("/cached"), [])
        with self.assertRaisesRegex(scraper.StageStop, "storage_budget"):
            c.client.get("/two")
        self.assertEqual(len(wire.calls), 1)

    def test_resume_full_run_uses_no_network(self):
        self.args.page_size = 1
        c, _ = self.collector(self.full_route)
        c.contexts = [{"election": "426", "year": "2024", "municipality": "12345"}]
        self.assertEqual(c.run(), 0)
        resumed, wire = self.collector(lambda _: self.fail("Resume should hit cache"), "resumed")
        resumed.contexts = c.contexts
        self.assertEqual(resumed.run(), 0)
        resumed.export()
        self.assertEqual(wire.calls, [])
        self.assertEqual(len(resumed.report["exports"]), 4)
        self.assertEqual(resumed.seeds, c.seeds)
        self.assertEqual(resumed.deferred, c.deferred)

    def test_no_retry_on_404(self):
        self.args.retries = 2
        c, wire = self.collector(lambda _: http_error(404))
        self.begin(c)
        self.assertIsNone(c.client.get("/test"))
        self.assertEqual(len(wire.calls), 1)

    def test_invalid_json_never_cached(self):
        c, _ = self.collector(lambda _: None)
        def invalid(*_, **__):
            return Response(b'<html>not JSON</html>')
        c.client.opener = invalid
        stats = self.begin(c)
        self.assertIsNone(c.client.get("/test"))
        self.assertIsNone(self.store.get("https://example.test/api/test"))
        self.assertEqual(stats["failed"], 1)

    def test_response_size_limit(self):
        self.args.max_response_bytes = 3
        c, _ = self.collector(lambda _: {"too": "big"})
        self.begin(c)
        self.assertIsNone(c.client.get("/test"))
        self.assertIsNone(self.store.get("https://example.test/api/test"))

    def test_failure_circuit_breaker(self):
        self.args.failure_limit = 2
        c, _ = self.collector(lambda _: http_error(503))
        self.begin(c)
        c.client.get("/one")
        with self.assertRaisesRegex(scraper.StageStop, "consecutive_failures"):
            c.client.get("/two")

    def test_request_budget_and_storage_budget(self):
        self.args.max_requests = 1
        c, _ = self.collector(lambda _: {})
        self.begin(c)
        c.client.get("/one")
        with self.assertRaisesRegex(scraper.StageStop, "request_budget"):
            c.client.get("/two")
        self.args.max_requests = 0
        self.args.max_data_bytes = 1
        with self.assertRaisesRegex(scraper.StageStop, "storage_budget"):
            c.client.get("/three")

    def test_pacing_applies_to_retries(self):
        self.args.delay, self.args.retries = 5, 1
        ticks = [0]
        sleeps = []
        responses = iter([http_error(503), {}, {}])
        c, _ = self.collector(lambda _: next(responses))
        c.client.clock = lambda: ticks[0]
        def sleep(delay):
            sleeps.append(delay)
            ticks[0] += delay
        c.client.sleep = sleep
        self.begin(c)
        c.client.get("/one")
        c.client.get("/two")
        self.assertEqual(ticks[0], 10)

    def full_route(self, url):
        parsed = urlsplit(url)
        path = parsed.path.removeprefix("/api")
        query = parse_qs(parsed.query)
        if path == "/eleicoes/anos-eleitorais":
            return [2024]
        if path == "/eleicoes/ordinarias":
            return [{"id": 426, "ano": 2024}]
        if path == "/eleicoes/cargos-por-municipio":
            return {"cargos": [{"codigo": 11}]}
        if path == "/eleicoes/candidaturas":
            return {"candidatos": [{"id": "100"}]}
        if path == "/eleicoes/candidaturas/100":
            return {"id": "100", "cnpj": "00.000.684/0001-21"}
        if path == "/cnpj/v1/00000684000121":
            return {"cnpj": "00000684000121"}
        if path == "/fipe/tabelas/v1":
            return [{"codigo": 1}, {"codigo": 2}]
        if path == "/fipe/marcas/v1" or path.endswith("/motos") or path.endswith("/caminhoes"):
            return []
        if path == "/fipe/marcas/v1/carros":
            return [{"nome": "Synthetic brand", "valor": "10"}]
        if path == "/fipe/veiculos/v1/carros/10":
            return [{"modelo": "Synthetic model", "valor": "20"}]
        if path == "/fipe/anos/v1/carros/10/20":
            return [{"nome": "Year one", "valor": "2001-1"}, {"nome": "Year two", "valor": "2002-1"}]
        if path.startswith("/fipe/detalhes/v1/carros/10/20/"):
            return {"codigoFipe": "000001-1", "valor": "R$ 1,00"}
        if path == "/fipe/preco/v1/000001-1":
            return [{"codigoFipe": "000001-1", "valor": "R$ 1,00"}]
        if path == "/cvm/fundos/v1":
            page = int(query["page"][0])
            rows = [{"cnpj": "12.345.678/0001-95"}] if page == 1 else []
            return {"data": rows, "page": page, "size": int(query["size"][0])}
        if path == "/cvm/fundos/v1/12345678000195":
            return {"cnpj": "12.345.678/0001-95", "cnpj_administrador": "11.111.111/0001-11"}
        self.fail(f"Unexpected request: {url}")

    def test_full_traversal_order_dedup_and_export(self):
        self.args.page_size = 1
        c, wire = self.collector(self.full_route)
        c.contexts = [{"election": "426", "year": "2024", "municipality": "12345"}]
        self.assertEqual(c.run(), 0)
        groups = []
        for url in wire.calls:
            first = urlsplit(url).path.split("/")[2]
            groups.append({"eleicoes": 0, "cnpj": 1, "fipe": 2, "cvm": 3}[first])
        self.assertEqual(groups, sorted(groups))
        self.assertEqual(len([u for u in wire.calls if "/fipe/preco/" in u]), 2)  # once per table
        fipe_queries = [parse_qs(urlsplit(u).query) for u in wire.calls
                        if "/fipe/" in u and "/tabelas/" not in u]
        self.assertTrue(all(q["tabela_referencia"] in (["1"], ["2"]) for q in fipe_queries))
        self.assertIn("12345678000195", c.deferred)
        self.assertFalse(any("/cnpj/v1/12345678000195" in u for u in wire.calls))
        self.assertTrue(c.report["stages"]["fundos_cvm"]["pagination_exhausted"])
        c.export()
        report = json.loads((Path(self.args.output) / "manifest.json").read_text())
        self.assertEqual(len(report["exports"]), 4)
        for entry in report["exports"]:
            path = Path(self.args.output) / entry["path"]
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), entry["sha256"])
            with gzip.open(path, "rt") as stream:
                for line in stream:
                    row = json.loads(line)
                    self.assertEqual(scraper.digest(row["data"]), row["sha256"])
        self.assertEqual(report["status"], "complete_for_scope")

    def test_latest_table_only(self):
        self.args.fipe_tables = "latest"
        c, wire = self.collector(self.full_route)
        self.begin(c)
        c.current = {"limitations": []}
        c.fipe()
        self.assertEqual(c.current["reference_tables"], [2])
        self.assertFalse(any("tabela_referencia=1" in u for u in wire.calls))

    def test_missing_inputs_are_not_silently_complete(self):
        c, _ = self.collector(self.full_route)
        self.assertEqual(c.run(), 2)
        self.assertEqual(c.report["stages"]["cnpj"]["status"], "blocked")
        self.assertEqual(c.report["stages"]["eleicoes"]["status"], "partial")

    def test_export_budget_is_explicit_and_preserves_cache(self):
        self.args.export_max_bytes = 1
        c, _ = self.collector(self.full_route)
        c.run()
        c.export()
        self.assertEqual(c.report["exports"], [])
        self.assertTrue(c.report["stages"]["fipe"]["export_truncated"])
        self.assertIsNotNone(self.store.get("https://example.test/api/fipe/tabelas/v1"))

    def test_partial_and_empty_cvm_page_termination(self):
        for funds in ([], [{"cnpj": "00.000.684/0001-21"}]):
            with self.subTest(funds=funds):
                def route(url):
                    if "?" in url:
                        return {"data": funds, "page": 1, "size": 200}
                    return {"cnpj": "00000684000121"}
                # Change base URL to avoid sharing cached variants between subtests.
                self.args.base_url += "/case"
                c, wire = self.collector(route)
                self.begin(c, "fundos_cvm")
                c.current = {"limitations": []}
                c.fundos_cvm()
                self.assertTrue(c.current["pagination_exhausted"])
                self.assertEqual(len([u for u in wire.calls if "?" in u]), 1)

    def test_repeated_cvm_page_fails_instead_of_looping(self):
        self.args.page_size = 1
        def route(url):
            query = parse_qs(urlsplit(url).query)
            if query:
                return {"data": [{"cnpj": "00.000.684/0001-21"}],
                        "page": int(query["page"][0]), "size": 1}
            return {"cnpj": "00000684000121"}
        c, wire = self.collector(route)
        self.begin(c, "fundos_cvm")
        c.current = {"limitations": []}
        with self.assertRaisesRegex(ValueError, "Repeated CVM page"):
            c.fundos_cvm()
        self.assertEqual(len(wire.calls), 3)

    def test_wrong_pagination_metadata_is_rejected(self):
        c, _ = self.collector(lambda _: {"data": [], "page": 0, "size": 200})
        self.begin(c, "fundos_cvm")
        c.current = {"limitations": []}
        with self.assertRaisesRegex(ValueError, "pagination metadata"):
            c.fundos_cvm()

    def test_failed_page_does_not_skip_to_later_page(self):
        c, wire = self.collector(lambda _: http_error(503))
        self.begin(c, "fundos_cvm")
        c.current = {"limitations": []}
        c.fundos_cvm()
        self.assertEqual(len(wire.calls), 1)
        self.assertFalse(c.current["pagination_exhausted"])
        self.assertTrue(c.current["limitations"])

    def test_input_validation_happens_before_network(self):
        path = self.root / "cnpjs.txt"
        path.write_text("invalid-cnpj\n")
        self.args.cnpj_file = str(path)
        c, wire = self.collector(lambda _: self.fail("network used before validation"))
        with self.assertRaises(ValueError):
            c.load_inputs()
        self.assertEqual(wire.calls, [])

    def test_municipality_inputs_expand_ordinary_elections(self):
        c, wire = self.collector(self.full_route)
        c.municipalities = ["12345"]
        self.begin(c, "eleicoes")
        c.current = {"limitations": []}
        c.eleicoes()
        self.assertEqual(c.current["limitations"], [])
        self.assertIn("00000684000121", c.seeds)
        self.assertTrue(any("election=426" in u and "municipality=12345" in u for u in wire.calls))

    def test_transport_failure_run_has_no_success_exports(self):
        c, wire = self.collector(lambda _: urllib.error.URLError("synthetic failure"))
        self.assertEqual(c.run(), 2)
        c.export()
        self.assertEqual(c.report["exports"], [])
        self.assertEqual(list(c.report["stages"]), list(scraper.STAGES))
        self.assertEqual(len(wire.calls), 4)
        self.assertTrue((Path(self.args.output) / "errors.jsonl").exists())

    def test_interrupted_run_exports_prior_success(self):
        responses = iter([[2024], KeyboardInterrupt()])
        c, _ = self.collector(lambda _: [])
        def opener(*_, **__):
            value = next(responses)
            if isinstance(value, BaseException):
                raise value
            return Response(json.dumps(value).encode())
        c.client.opener = opener
        self.assertEqual(c.run(), 130)
        c.export()
        self.assertEqual(c.report["stages"]["cnpj"]["status"], "not_started")
        self.assertEqual(c.report["stages"]["eleicoes"]["exported_responses"], 1)


if __name__ == "__main__":
    unittest.main()
