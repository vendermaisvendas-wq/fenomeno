"""
Diagnóstico completo do sistema BuscaPlace.

Verifica:
  1. Conexão com banco de dados
  2. Existência das tabelas principais
  3. Contagem de watchers e listings
  4. Funcionamento do keyword_expander
  5. Funcionamento do discovery engine (teste real com DDG)
  6. Funcionamento do parser de HTML

Uso:
    python system_check.py
    python system_check.py --skip-network    # pula testes que precisam de internet
"""

from __future__ import annotations

import argparse
import sys
import time


def _check(name: str, fn, skip: bool = False):
    if skip:
        print(f"  [{name:30s}] PULADO")
        return
    try:
        result = fn()
        print(f"  [{name:30s}] OK — {result}")
    except Exception as e:
        print(f"  [{name:30s}] FALHOU — {type(e).__name__}: {e}")


def check_database():
    from db import connect, init_db
    init_db()
    with connect() as conn:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
    expected = {"listings", "snapshots", "events", "watchers",
                "watcher_results", "price_history"}
    missing = expected - tables
    if missing:
        return f"tabelas encontradas mas FALTAM: {missing}"
    return f"{len(tables)} tabelas OK"


def check_listings():
    from db import connect
    with connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
        active = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE is_removed = 0"
        ).fetchone()[0]
    return f"{total} total, {active} ativos"


def check_watchers():
    from db import connect
    with connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM watchers").fetchone()[0]
        active = conn.execute(
            "SELECT COUNT(*) FROM watchers WHERE is_active = 1"
        ).fetchone()[0]
    return f"{total} total, {active} ativos"


def check_events():
    from db import connect
    with connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        alerts = conn.execute(
            "SELECT COUNT(*) FROM events WHERE event_type = 'alert_sent'"
        ).fetchone()[0]
    return f"{total} eventos, {alerts} alertas enviados"


def check_keyword_expander():
    from keyword_expander import expand
    results = expand("iphone")
    return f"{len(results)} variações para 'iphone': {results[:4]}"


def check_discovery_engine(keyword="iphone", region=None):
    from marketplace_discovery_engine import discover_for
    result = discover_for(keyword=keyword, region=region, max_pages=1,
                          max_variations=2, use_cache=False)
    n = result.get("total_unique_hits", 0)
    queries = result.get("queries_run", 0)
    cache = result.get("cache_hits", 0)
    if n == 0:
        return (f"0 resultados (queries={queries}, cache={cache}) — "
                "DDG pode não ter retornado URLs de marketplace")
    return f"{n} anúncios encontrados (queries={queries})"


def check_extract(listing_id="2015275022700246"):
    from extract_item import extract
    listing = extract(listing_id)
    if listing.status == "ok":
        return f"status=ok título='{(listing.title or '')[:50]}'"
    return f"status={listing.status} método={listing.extraction_method}"


def check_templates():
    from pathlib import Path
    tdir = Path(__file__).parent / "templates"
    files = list(tdir.glob("*.html"))
    return f"{len(files)} templates em {tdir}"


def check_imports():
    modules = [
        "db", "extract_item", "monitor", "web", "watcher_engine",
        "marketplace_discovery_engine", "keyword_expander",
        "discover_links", "alerts", "alert_engine",
    ]
    ok = []
    fail = []
    for m in modules:
        try:
            __import__(m)
            ok.append(m)
        except Exception as e:
            fail.append(f"{m}({type(e).__name__})")
    if fail:
        return f"{len(ok)} OK, FALHAS: {fail}"
    return f"{len(ok)} módulos importados OK"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--skip-network", action="store_true",
                    help="pula testes que precisam de internet")
    args = ap.parse_args()

    print("=" * 60)
    print("  DIAGNÓSTICO DO SISTEMA — BuscaPlace")
    print("=" * 60)
    print()

    _check("Importação de módulos", check_imports)
    _check("Banco de dados", check_database)
    _check("Templates HTML", check_templates)
    _check("Listings no banco", check_listings)
    _check("Monitoramentos (watchers)", check_watchers)
    _check("Eventos e alertas", check_events)
    _check("Expansor de keywords", check_keyword_expander)

    print()
    print("--- Testes com internet ---")
    _check("Discovery engine (DDG)", lambda: check_discovery_engine(), skip=args.skip_network)
    _check("Extrator de HTML (FB)", lambda: check_extract(), skip=args.skip_network)

    print()
    print("=" * 60)
    print("  Diagnóstico concluído")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
