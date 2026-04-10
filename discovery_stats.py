"""
Estatísticas de cobertura de discovery.

Calcula:
    - Anúncios por keyword (top tokens)
    - Anúncios por região (top cidades)
    - Taxa de novos anúncios por dia (últimos N dias)
    - Hit ratio do discovery_cache

Não persiste em tabela — é todo computado on-demand. Para volumes grandes,
considere migrar para uma view materializada (não implementada aqui).

Uso:
    python discovery_stats.py
    python discovery_stats.py --json
    python discovery_stats.py --top 30 --days 14
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

from db import connect, init_db
from logging_setup import get_logger
from title_normalizer import tokens

log = get_logger("discovery_stats")


@dataclass
class StatsReport:
    by_keyword: list[dict]
    by_region: list[dict]
    detection_rate: dict
    cache_summary: dict


def stats_by_keyword(top: int = 20) -> list[dict]:
    init_db()
    with connect() as conn:
        rows = conn.execute(
            "SELECT current_title FROM listings WHERE is_removed = 0 "
            "AND current_title IS NOT NULL"
        ).fetchall()
    counts: Counter[str] = Counter()
    for r in rows:
        for tok in tokens(r["current_title"]):
            counts[tok] += 1
    return [
        {"keyword": k, "count": c}
        for k, c in counts.most_common(top)
    ]


def stats_by_region(top: int = 20) -> list[dict]:
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


def detection_rate(days: int = 7) -> dict:
    init_db()
    threshold = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat(timespec="seconds")
    with connect() as conn:
        new_count = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE first_seen_at >= ?",
            (threshold,),
        ).fetchone()[0]
        total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
        # Quebra por dia
        per_day = conn.execute(
            """
            SELECT SUBSTR(first_seen_at, 1, 10) AS day, COUNT(*) AS n
              FROM listings
             WHERE first_seen_at >= ?
             GROUP BY day ORDER BY day
            """,
            (threshold,),
        ).fetchall()
    return {
        "days": days,
        "new_listings": new_count,
        "total_listings": total,
        "rate_per_day": round(new_count / max(days, 1), 2),
        "per_day": [dict(r) for r in per_day],
    }


def cache_summary() -> dict:
    init_db()
    with connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM discovery_cache").fetchone()[0]
        live = conn.execute(
            "SELECT COUNT(*) FROM discovery_cache WHERE expires_at > datetime('now')"
        ).fetchone()[0]
    return {"total": total, "live": live, "expired": total - live}


def build_report(top: int = 20, days: int = 7) -> StatsReport:
    return StatsReport(
        by_keyword=stats_by_keyword(top),
        by_region=stats_by_region(top),
        detection_rate=detection_rate(days),
        cache_summary=cache_summary(),
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    report = build_report(top=args.top, days=args.days)
    if args.json:
        print(json.dumps(asdict(report), ensure_ascii=False, indent=2))
        return 0

    print("=== Top keywords ===")
    for r in report.by_keyword:
        print(f"  {r['keyword']:<20s} {r['count']:>6d}")

    print("\n=== Top regions ===")
    for r in report.by_region:
        print(f"  {(r['city'] or '')[:25]:<25s} {(r['state'] or '-'):3s}  {r['active_count']:>6d}")

    print("\n=== Detection rate ===")
    dr = report.detection_rate
    print(f"  novos em {dr['days']} dias: {dr['new_listings']}")
    print(f"  total na base:        {dr['total_listings']}")
    print(f"  rate/day:             {dr['rate_per_day']}")

    print("\n=== Discovery cache ===")
    cs = report.cache_summary
    print(f"  total: {cs['total']}  live: {cs['live']}  expired: {cs['expired']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
