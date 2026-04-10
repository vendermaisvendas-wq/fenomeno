"""
Profiler do pipeline: mede o tempo de cada etapa do sistema, para um
baseline de performance e para identificar gargalos.

Não faz fetches reais contra o Facebook — só exercita os módulos que
operam sobre o banco local (market_value, outlier, cluster, score,
dup_detector, fraud, new_detector, alerts em dry-run).

Opcionalmente inclui `extract_item.extract()` de uma URL fornecida, e
`discover_links` numa palavra-chave — estes sim fazem tráfego de rede.

Uso:
    python profile_pipeline.py                          # só módulos locais
    python profile_pipeline.py --extract <ID_OR_URL>    # inclui 1 fetch
    python profile_pipeline.py --discover <keyword>     # inclui DDG search
    python profile_pipeline.py --json
"""

from __future__ import annotations

import argparse
import json
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from typing import Callable

from logging_setup import configure as configure_logging, get_logger, kv

log = get_logger("profile")


@dataclass
class Stage:
    name: str
    ok: bool
    elapsed_ms: float
    note: str = ""


@contextmanager
def _timer(name: str, results: list[Stage]):
    t0 = time.perf_counter()
    note = ""
    ok = True
    try:
        yield lambda n: setattr(results[-1], "note", n) if results else None
    except Exception as e:  # noqa: BLE001
        ok = False
        note = f"ERROR: {type(e).__name__}: {e}"
    finally:
        elapsed = (time.perf_counter() - t0) * 1000
        results.append(Stage(name=name, ok=ok, elapsed_ms=round(elapsed, 2), note=note))


def _run(name: str, fn: Callable[[], dict | None], results: list[Stage]) -> None:
    """Roda fn() cronometrando e capturando resultado/erro. Não propaga."""
    t0 = time.perf_counter()
    try:
        result = fn()
        note = ""
        if isinstance(result, dict):
            # Primeira key interessante para o log
            parts = [f"{k}={v}" for k, v in list(result.items())[:3]]
            note = " ".join(parts)
        ok = True
    except Exception as e:  # noqa: BLE001
        ok = False
        note = f"ERROR: {type(e).__name__}: {e}"
    elapsed = (time.perf_counter() - t0) * 1000
    results.append(Stage(name=name, ok=ok, elapsed_ms=round(elapsed, 2), note=note))
    log.info(kv(stage=name, ok=ok, ms=round(elapsed, 2)))


def profile_local_stages() -> list[Stage]:
    results: list[Stage] = []

    # db init (sanity check)
    def _db_init():
        from db import init_db
        init_db()
        return None
    _run("db.init_db", _db_init, results)

    # outliers ANTES de market_value (ordem correta)
    def _outliers():
        from outlier_detector import detect_outliers
        return detect_outliers(dry_run=True)
    _run("outlier_detector.detect_outliers[dry]", _outliers, results)

    def _market():
        from market_value import recompute_all
        return recompute_all(dry_run=True)
    _run("market_value.recompute_all[dry]", _market, results)

    def _listing_cluster():
        from listing_cluster import cluster_all
        return cluster_all(dry_run=True)
    _run("listing_cluster.cluster_all[dry]", _listing_cluster, results)

    def _dup():
        from duplicate_detector import cluster_all as dup_cluster
        return dup_cluster(dry_run=True)
    _run("duplicate_detector.cluster_all[dry]", _dup, results)

    def _fraud():
        from fraud_detector import scan
        return scan(dry_run=True)
    _run("fraud_detector.scan[dry]", _fraud, results)

    def _opps_scan():
        from opportunities import scan
        return scan(dry_run=True)
    _run("opportunities.scan[dry]", _opps_scan, results)

    def _score():
        # Não há flag dry aqui (UPDATE é idempotente); medimos no modo real
        from opportunities import score_all_listings
        return {"updated": score_all_listings()}
    _run("opportunities.score_all_listings", _score, results)

    def _new():
        from new_listing_detector import scan as new_scan
        return new_scan(dry_run=True)
    _run("new_listing_detector.scan[dry]", _new, results)

    def _alerts():
        from alerts import scan_and_alert
        return scan_and_alert(dry_run=True)
    _run("alerts.scan_and_alert[dry]", _alerts, results)

    def _velocity():
        from sales_velocity import compute_global
        stats = compute_global()
        return {"n": stats.removed_count} if stats else {"n": 0}
    _run("sales_velocity.compute_global", _velocity, results)

    return results


def profile_extract(target: str) -> Stage:
    results: list[Stage] = []
    def _extract():
        from extract_item import extract
        l = extract(target)
        return {"status": l.status, "method": l.extraction_method}
    _run(f"extract_item.extract({target})", _extract, results)
    return results[0]


def profile_discover(keyword: str) -> Stage:
    results: list[Stage] = []
    def _discover():
        from discover_links import DuckDuckGoBackend, discover
        hits = discover([keyword], DuckDuckGoBackend(delay_range=(1.0, 2.0)), max_pages=1)
        return {"hits": len(hits)}
    _run(f"discover_links.discover({keyword})", _discover, results)
    return results[0]


def _print_report(stages: list[Stage]) -> None:
    total = sum(s.elapsed_ms for s in stages)
    print(f"{'stage':45s} {'ms':>10s}  status  note")
    print("-" * 100)
    for s in stages:
        status = "ok " if s.ok else "FAIL"
        print(f"{s.name:45s} {s.elapsed_ms:10.2f}  {status:5s}  {s.note[:40]}")
    print("-" * 100)
    print(f"{'TOTAL':45s} {total:10.2f}  ms")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--extract", help="ID ou URL de item a extrair (fetch real)")
    ap.add_argument("--discover", help="keyword para DuckDuckGo (fetch real)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    configure_logging()
    stages = profile_local_stages()
    if args.extract:
        stages.append(profile_extract(args.extract))
    if args.discover:
        stages.append(profile_discover(args.discover))

    if args.json:
        print(json.dumps([asdict(s) for s in stages], indent=2))
    else:
        _print_report(stages)
    return 0 if all(s.ok for s in stages) else 1


if __name__ == "__main__":
    raise SystemExit(main())
