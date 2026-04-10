"""
Healthcheck do parser: roda extract_item em uma amostra de listings já
conhecidos e mede qual camada preencheu qual campo. Quando uma camada cai
para 0% sem o restante cair junto, é sinal forte de que o FB mudou o HTML
daquela camada específica — você sabe exatamente por onde começar a correção.

Entrada: amostra aleatória de N listings ativos do DB (default 10).

Saída:
  - Tabela por camada: coverage % do universo amostrado
  - Tabela por campo: qual camada preencheu (com fallback)
  - Contagem de statuses (ok / login_wall / not_found / empty / error)
  - Lista de URLs com status != ok para triagem

Uso:
    python parser_health.py
    python parser_health.py --sample 20
    python parser_health.py --from-cache      # usa HTML do html_cache/ (offline)
    python parser_health.py --json
"""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from dataclasses import asdict

from db import connect, init_db, random_active_ids
from extract_item import extract, parse_html, ITEM_URL
from html_cache import cached_ids, load_html
from logging_setup import configure as configure_logging, get_logger, kv

log = get_logger("parser_health")

LAYERS = ("jsonld", "og", "relay", "json_walk", "dom")
TRACKED_FIELDS = (
    "title", "price_amount", "price_currency", "price_formatted",
    "description", "location_text", "category", "creation_time",
    "primary_image_url",
)


def _collect(sample_size: int, from_cache: bool) -> list:
    """Retorna lista de Listing objects extraídos na amostra."""
    listings = []

    if from_cache:
        ids = cached_ids()[:sample_size]
        if not ids:
            print("[warn] html_cache/ vazio — rode extract_item.py --debug-html antes")
            return []
        for item_id in ids:
            html = load_html(item_id)
            if html is None:
                continue
            url = ITEM_URL.format(id=item_id)
            listings.append(parse_html(html, item_id, url))
        return listings

    init_db()
    with connect() as conn:
        rows = random_active_ids(conn, limit=sample_size)
    for row in rows:
        listing = extract(row["url"])
        listings.append(listing)
        time.sleep(4)  # rate limit conservador — healthcheck não é urgente
    return listings


def _layer_coverage(listings: list) -> dict[str, float]:
    """% de listings OK em que cada camada aparece pelo menos uma vez."""
    ok_listings = [l for l in listings if l.status == "ok"]
    if not ok_listings:
        return {layer: 0.0 for layer in LAYERS}
    cov = {}
    for layer in LAYERS:
        n = sum(1 for l in ok_listings if layer in (l.extraction_method or ""))
        cov[layer] = n / len(ok_listings) * 100
    return cov


def _field_coverage(listings: list) -> dict[str, dict]:
    """Para cada campo: {covered_pct, per_layer: {layer: count}}."""
    ok_listings = [l for l in listings if l.status == "ok"]
    total = len(ok_listings) or 1
    out: dict[str, dict] = {}
    for fld in TRACKED_FIELDS:
        covered = sum(1 for l in ok_listings if getattr(l, fld, None))
        per_layer: Counter[str] = Counter()
        for l in ok_listings:
            src = l.field_sources.get(fld)
            if src:
                per_layer[src] += 1
        out[fld] = {
            "covered_pct": covered / total * 100,
            "per_layer": dict(per_layer),
        }
    return out


def build_report(listings: list) -> dict:
    statuses = Counter(l.status for l in listings)
    non_ok = [
        {"id": l.id, "status": l.status, "method": l.extraction_method, "url": l.url}
        for l in listings if l.status != "ok"
    ]
    return {
        "sample_size": len(listings),
        "statuses": dict(statuses),
        "layer_coverage_pct": _layer_coverage(listings),
        "field_coverage": _field_coverage(listings),
        "non_ok_items": non_ok,
    }


def print_report(report: dict) -> None:
    print(f"sample_size: {report['sample_size']}")
    print(f"statuses:    {report['statuses']}")
    print()
    print("layer coverage (% of OK listings that used this layer):")
    for layer, pct in report["layer_coverage_pct"].items():
        bar = "█" * int(pct / 5)
        print(f"  {layer:10s} {pct:6.1f}%  {bar}")
    print()
    print("field coverage (% of OK listings that have this field, + source layers):")
    for fld, data in report["field_coverage"].items():
        sources = ", ".join(f"{k}={v}" for k, v in sorted(data["per_layer"].items()))
        print(f"  {fld:22s} {data['covered_pct']:6.1f}%   [{sources or 'none'}]")
    if report["non_ok_items"]:
        print()
        print("non-ok items (triage):")
        for it in report["non_ok_items"]:
            print(f"  {it['status']:12s} {it['id']}  ({it['method']})")
            print(f"    {it['url']}")


def _verdict(report: dict) -> str:
    """Veredito grosseiro: 'healthy' / 'degraded' / 'broken'."""
    ok_rate = report["statuses"].get("ok", 0) / max(report["sample_size"], 1)
    if ok_rate < 0.5:
        return "broken"
    if ok_rate < 0.85:
        return "degraded"
    # Se a camada OG caiu abaixo de 50% é sinal forte de quebra parcial
    og = report["layer_coverage_pct"].get("og", 0)
    if og < 50:
        return "degraded"
    return "healthy"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sample", type=int, default=10,
                    help="tamanho da amostra (default: 10)")
    ap.add_argument("--from-cache", action="store_true",
                    help="parseia a partir de html_cache/ ao invés de fazer fetch")
    ap.add_argument("--json", action="store_true", help="saída JSON")
    args = ap.parse_args()

    configure_logging()

    listings = _collect(args.sample, from_cache=args.from_cache)
    if not listings:
        print("[error] nenhum listing na amostra")
        return 1

    report = build_report(listings)
    report["verdict"] = _verdict(report)

    log.info(kv(
        event="health_report",
        verdict=report["verdict"],
        sample=report["sample_size"],
        **report["statuses"],
    ))

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print_report(report)
        print(f"\nverdict: {report['verdict'].upper()}")

    return 0 if report["verdict"] == "healthy" else 1


if __name__ == "__main__":
    raise SystemExit(main())
