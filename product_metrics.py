"""
Métricas de performance do produto, focadas no funil
publicação → descoberta → alerta.

Métricas principais:
    - Anúncios descobertos por dia (últimos N dias)
    - Watcher matches por dia
    - Alertas enviados por dia
    - Tempo médio first_seen → alert_sent (proxy para "tempo até alerta")

NOTA HONESTA sobre "tempo entre publicação e alerta":
    O sistema NÃO tem acesso à hora real de publicação no FB. Usamos
    `first_seen_at` (= primeira vez que NÓS vimos o anúncio) como proxy.
    O delta first_seen → alert é o tempo de **processamento interno**, não
    o tempo real desde a publicação. O verdadeiro "publication → alert"
    sempre tem um piso adicional dado pela latência de indexação do DDG.

Uso:
    python product_metrics.py
    python product_metrics.py --days 30 --json
"""

from __future__ import annotations

import argparse
import json
import statistics
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

from db import connect, init_db
from logging_setup import get_logger

log = get_logger("product_metrics")


@dataclass
class ProductMetrics:
    days: int
    daily_new_listings: list[dict]
    daily_watcher_matches: list[dict]
    daily_alerts: list[dict]
    time_to_alert: dict | None
    totals: dict


def _parse(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return sorted_vals[f]
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def daily_new_listings(days: int) -> list[dict]:
    init_db()
    threshold = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat(timespec="seconds")
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT SUBSTR(first_seen_at, 1, 10) AS day, COUNT(*) AS n
              FROM listings
             WHERE first_seen_at >= ?
             GROUP BY day ORDER BY day
            """,
            (threshold,),
        ).fetchall()
    return [dict(r) for r in rows]


def daily_watcher_matches(days: int) -> list[dict]:
    init_db()
    threshold = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat(timespec="seconds")
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT SUBSTR(at, 1, 10) AS day, COUNT(*) AS n
              FROM events
             WHERE event_type = 'watcher_match' AND at >= ?
             GROUP BY day ORDER BY day
            """,
            (threshold,),
        ).fetchall()
    return [dict(r) for r in rows]


def daily_alerts(days: int) -> list[dict]:
    init_db()
    threshold = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat(timespec="seconds")
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT SUBSTR(at, 1, 10) AS day, COUNT(*) AS n
              FROM events
             WHERE event_type = 'alert_sent' AND at >= ?
             GROUP BY day ORDER BY day
            """,
            (threshold,),
        ).fetchall()
    return [dict(r) for r in rows]


def time_to_alert_distribution(days: int = 30) -> dict | None:
    """Para cada alert_sent recente, calcula delta vs first_seen do listing.
    Retorna percentis e mediana em minutos. Veja NOTA no docstring do módulo
    sobre o que esse delta significa de verdade."""
    init_db()
    threshold = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat(timespec="seconds")
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT e.at AS alert_at, l.first_seen_at
              FROM events e
              JOIN listings l ON l.id = e.listing_id
             WHERE e.event_type = 'alert_sent'
               AND e.at >= ?
            """,
            (threshold,),
        ).fetchall()

    deltas: list[float] = []
    for r in rows:
        try:
            alert_dt = _parse(r["alert_at"])
            seen_dt = _parse(r["first_seen_at"])
            d = (alert_dt - seen_dt).total_seconds() / 60.0
            if d >= 0:
                deltas.append(d)
        except (ValueError, TypeError):
            continue

    if not deltas:
        return None

    sorted_d = sorted(deltas)
    return {
        "n": len(deltas),
        "min_minutes": round(sorted_d[0], 2),
        "median_minutes": round(statistics.median(sorted_d), 2),
        "mean_minutes": round(statistics.fmean(sorted_d), 2),
        "p25_minutes": round(_percentile(sorted_d, 25), 2),
        "p75_minutes": round(_percentile(sorted_d, 75), 2),
        "p90_minutes": round(_percentile(sorted_d, 90), 2),
        "max_minutes": round(sorted_d[-1], 2),
    }


def totals() -> dict:
    init_db()
    with connect() as conn:
        listings_total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
        listings_active = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE is_removed = 0"
        ).fetchone()[0]
        watchers_active = conn.execute(
            "SELECT COUNT(*) FROM watchers WHERE is_active = 1"
        ).fetchone()[0]
        wm = conn.execute(
            "SELECT COUNT(*) FROM events WHERE event_type = 'watcher_match'"
        ).fetchone()[0]
        alerts = conn.execute(
            "SELECT COUNT(*) FROM events WHERE event_type = 'alert_sent'"
        ).fetchone()[0]
    return {
        "listings_total": listings_total,
        "listings_active": listings_active,
        "watchers_active": watchers_active,
        "watcher_matches_total": wm,
        "alerts_sent_total": alerts,
    }


def coverage_by_region(top: int = 15) -> list[dict]:
    """v10: cobertura por região (cidades com mais listings ativos)."""
    init_db()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT city, state, COUNT(*) AS active_count
              FROM listings
             WHERE is_removed = 0 AND city IS NOT NULL
             GROUP BY city, state
             ORDER BY active_count DESC
             LIMIT ?
            """,
            (top,),
        ).fetchall()
    return [dict(r) for r in rows]


def discovery_rate(days: int) -> dict:
    """v10: taxa de descoberta = novos por dia / total ativos."""
    init_db()
    threshold = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat(timespec="seconds")
    with connect() as conn:
        new_count = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE first_seen_at >= ?",
            (threshold,),
        ).fetchone()[0]
        active = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE is_removed = 0"
        ).fetchone()[0]
    return {
        "days": days,
        "new_listings": new_count,
        "active_total": active,
        "rate_per_day": round(new_count / max(days, 1), 2),
        "growth_pct": round(new_count / max(active, 1) * 100, 2),
    }


def build(days: int = 14) -> ProductMetrics:
    return ProductMetrics(
        days=days,
        daily_new_listings=daily_new_listings(days),
        daily_watcher_matches=daily_watcher_matches(days),
        daily_alerts=daily_alerts(days),
        time_to_alert=time_to_alert_distribution(days=days),
        totals=totals(),
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    metrics = build(days=args.days)
    if args.json:
        print(json.dumps(asdict(metrics), ensure_ascii=False, indent=2))
        return 0

    print("=== Totals ===")
    for k, v in metrics.totals.items():
        print(f"  {k:25s} {v}")

    print(f"\n=== Daily activity (últimos {metrics.days} dias) ===")
    print(f"  {'day':12s}  {'new':>6s}  {'matches':>8s}  {'alerts':>7s}")
    by_day: dict[str, dict] = {}
    for r in metrics.daily_new_listings:
        by_day.setdefault(r["day"], {})["new"] = r["n"]
    for r in metrics.daily_watcher_matches:
        by_day.setdefault(r["day"], {})["matches"] = r["n"]
    for r in metrics.daily_alerts:
        by_day.setdefault(r["day"], {})["alerts"] = r["n"]
    for day in sorted(by_day):
        d = by_day[day]
        print(f"  {day:12s}  {d.get('new', 0):>6d}  "
              f"{d.get('matches', 0):>8d}  {d.get('alerts', 0):>7d}")

    print("\n=== Tempo first_seen → alert (proxy interno) ===")
    if metrics.time_to_alert is None:
        print("  (sem alertas no período)")
    else:
        for k, v in metrics.time_to_alert.items():
            print(f"  {k:18s} {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
