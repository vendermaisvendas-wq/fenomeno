"""
Detecta automaticamente quando o HTML do Marketplace mudou e quebrou o
parser. Roda periodicamente (ideal: diário) e compara a saúde atual contra
uma baseline histórica.

Fluxo:
  1. Chama parser_health.build_report() sobre uma amostra
  2. Grava na tabela `parser_health_history` (at, sample_size, ok_rate,
     <layer>_rate, verdict)
  3. Compara com a média móvel dos últimos N reports
  4. Se detectar queda abrupta (> DROP_THRESHOLD pp em ok_rate OU em
     qualquer layer individual), grava evento `parser_break` na tabela
     events com listing_id='*' e new_value descrevendo o delta.

Uso:
    python parser_regression_detector.py                # roda amostra 10, grava histórico + detecta
    python parser_regression_detector.py --sample 20
    python parser_regression_detector.py --from-cache   # não faz fetch, usa html_cache/
    python parser_regression_detector.py --history-only # só lista histórico, sem nova coleta
"""

from __future__ import annotations

import argparse
import statistics
from datetime import datetime, timezone

from db import connect, init_db, insert_event
from logging_setup import get_logger, kv
from parser_health import _collect, build_report

log = get_logger("parser_regression")

DROP_THRESHOLD_PP = 20.0      # queda percentual-point considerada "abrupta"
MIN_BASELINE_REPORTS = 3      # mínimo de reports anteriores para comparar
BASELINE_WINDOW = 10          # considera os últimos N reports como baseline

# ID sentinel para eventos de sistema (parser_break, etc.). Um row fantasma é
# criado on-demand para satisfazer o FK de events.listing_id → listings.id.
SYSTEM_LISTING_ID = "__system__"


def _ensure_system_listing(conn) -> None:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT OR IGNORE INTO listings
          (id, url, first_seen_at, last_seen_at, last_status)
        VALUES (?, '', ?, ?, 'system')
        """,
        (SYSTEM_LISTING_ID, now, now),
    )


def _current_rate(report: dict, layer: str) -> float:
    return report["layer_coverage_pct"].get(layer, 0.0)


def _ok_rate(report: dict) -> float:
    ok = report["statuses"].get("ok", 0)
    total = max(report["sample_size"], 1)
    return ok / total * 100.0


def _persist_report(conn, report: dict) -> None:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT INTO parser_health_history
          (at, sample_size, ok_rate, jsonld_rate, og_rate, relay_rate, dom_rate, verdict)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            now,
            report["sample_size"],
            round(_ok_rate(report), 2),
            round(_current_rate(report, "jsonld"), 2),
            round(_current_rate(report, "og"), 2),
            round(_current_rate(report, "relay"), 2),
            round(_current_rate(report, "dom"), 2),
            report.get("verdict", "unknown"),
        ),
    )


def _load_baseline(conn) -> list[dict]:
    """Últimos N reports históricos (excluindo o que acabamos de inserir)."""
    rows = conn.execute(
        """
        SELECT ok_rate, jsonld_rate, og_rate, relay_rate, dom_rate
          FROM parser_health_history
         ORDER BY at DESC
         LIMIT ?
        """,
        (BASELINE_WINDOW + 1,),  # +1 para pular o atual
    ).fetchall()
    return [dict(r) for r in rows[1:]]  # pula o mais recente (o atual)


def detect_regression(report: dict) -> tuple[bool, list[str]]:
    """Compara report novo contra baseline histórica. Retorna (broken, reasons)."""
    init_db()
    reasons: list[str] = []
    with connect() as conn:
        _persist_report(conn, report)
        baseline = _load_baseline(conn)

    if len(baseline) < MIN_BASELINE_REPORTS:
        log.info(kv(event="baseline_insufficient",
                    n=len(baseline), min=MIN_BASELINE_REPORTS))
        return False, ["baseline_insufficient"]

    # Compara cada métrica contra a média móvel
    def _avg(col: str) -> float:
        vals = [r[col] for r in baseline if r.get(col) is not None]
        return statistics.fmean(vals) if vals else 0.0

    metrics = {
        "ok_rate":     (_ok_rate(report),          _avg("ok_rate")),
        "og_rate":     (_current_rate(report, "og"),     _avg("og_rate")),
        "relay_rate":  (_current_rate(report, "relay"),  _avg("relay_rate")),
        "jsonld_rate": (_current_rate(report, "jsonld"), _avg("jsonld_rate")),
    }

    broken = False
    for name, (current, baseline_avg) in metrics.items():
        delta = baseline_avg - current  # queda positiva
        if delta >= DROP_THRESHOLD_PP:
            reasons.append(
                f"{name} dropped {delta:.1f}pp "
                f"(now={current:.1f}, baseline={baseline_avg:.1f})"
            )
            broken = True

    if broken:
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with connect() as conn:
            _ensure_system_listing(conn)
            insert_event(
                conn, listing_id=SYSTEM_LISTING_ID, at=now,
                event_type="parser_break",
                old_value=None, new_value="; ".join(reasons),
            )
        log.warning(kv(event="parser_break_detected", reasons=str(reasons)))
    else:
        log.info(kv(event="parser_healthy",
                    ok_rate=round(metrics["ok_rate"][0], 1),
                    baseline=round(metrics["ok_rate"][1], 1)))

    return broken, reasons


def print_history(limit: int = 20) -> None:
    init_db()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT at, sample_size, ok_rate, og_rate, relay_rate, verdict
              FROM parser_health_history
             ORDER BY at DESC LIMIT ?
            """,
            (limit,),
        ).fetchall()
    if not rows:
        print("(no history yet)")
        return
    print(f"{'at':20s} {'n':>4s} {'ok%':>6s} {'og%':>6s} {'relay%':>7s}  verdict")
    print("-" * 65)
    for r in rows:
        print(
            f"{r['at'][:19]:20s} {r['sample_size']:4d} "
            f"{r['ok_rate']:6.1f} {r['og_rate']:6.1f} "
            f"{r['relay_rate']:7.1f}  {r['verdict']}"
        )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sample", type=int, default=10)
    ap.add_argument("--from-cache", action="store_true")
    ap.add_argument("--history-only", action="store_true")
    args = ap.parse_args()

    if args.history_only:
        print_history()
        return 0

    listings = _collect(args.sample, from_cache=args.from_cache)
    if not listings:
        print("[error] amostra vazia")
        return 1
    report = build_report(listings)
    from parser_health import _verdict
    report["verdict"] = _verdict(report)

    broken, reasons = detect_regression(report)

    print(f"sample_size:  {report['sample_size']}")
    print(f"ok_rate:      {_ok_rate(report):.1f}%")
    print(f"og_rate:      {_current_rate(report, 'og'):.1f}%")
    print(f"relay_rate:   {_current_rate(report, 'relay'):.1f}%")
    print(f"verdict:      {report['verdict']}")
    if broken:
        print("\n⚠ REGRESSION DETECTED:")
        for r in reasons:
            print(f"  - {r}")
        return 2
    print("\n(baseline comparison OK)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
