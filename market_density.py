"""
Análise de densidade de mercado por token.

Para cada token comum no universo de listings, calcula:

    total_listings     : todos os listings (ativos + removidos) com o token
    active_listings    : só ativos agora
    removed_listings   : já removidos
    removal_rate       : removed / total
    avg_velocity_days  : mediana de (removed_at − first_seen_at) para os removidos
    competition_score  : 0..100 — quão "concorrido" é esse nicho

competition_score combina volume e velocidade:
    - volume alto    → muita concorrência (mais vendedores)
    - rotatividade alta → mercado líquido (boa demanda)

Usamos log-scale no volume para saturar em ~200 listings:

    volume_component     = min(60, 60 * log1p(active) / log1p(200))
    turnover_component   = int(40 * removal_rate)         # 0..40
    competition_score    = volume_component + turnover_component

Resultado:
    ALTO score → muita concorrência mas também alta liquidez (bom para vender rápido)
    BAIXO score → nicho mais raro, pode ser que venda lento OU seja oportunidade

Persistido em tabela `market_density` (overwrite completo a cada run).

Uso:
    python market_density.py
    python market_density.py --min-count 10 --top 30
"""

from __future__ import annotations

import argparse
import math
import statistics
from collections import defaultdict
from datetime import datetime, timezone

from db import connect, init_db, now_iso
from logging_setup import get_logger, kv
from title_normalizer import tokens

log = get_logger("market_density")

MIN_COUNT = 5


def _parse_dt(iso: str | None) -> datetime | None:
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _days_between(a: str | None, b: str | None) -> float | None:
    dt_a = _parse_dt(a)
    dt_b = _parse_dt(b)
    if dt_a is None or dt_b is None:
        return None
    return (dt_b - dt_a).total_seconds() / 86400.0


def _compute_competition_score(active: int, removal_rate: float) -> int:
    vol = min(60, int(60 * math.log1p(active) / math.log1p(200)))
    turnover = int(40 * max(0.0, min(1.0, removal_rate)))
    return min(100, vol + turnover)


def compute(min_count: int = MIN_COUNT) -> list[dict]:
    init_db()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT current_title, is_removed, first_seen_at, removed_at
              FROM listings
             WHERE current_title IS NOT NULL
            """
        ).fetchall()

    stats: dict[str, dict] = defaultdict(
        lambda: {"total": 0, "active": 0, "removed": 0, "velocity": []}
    )

    for r in rows:
        toks = tokens(r["current_title"])
        for tok in toks:
            s = stats[tok]
            s["total"] += 1
            if r["is_removed"]:
                s["removed"] += 1
                d = _days_between(r["first_seen_at"], r["removed_at"])
                if d is not None and d >= 0:
                    s["velocity"].append(d)
            else:
                s["active"] += 1

    results: list[dict] = []
    for tok, s in stats.items():
        if s["total"] < min_count:
            continue
        removal_rate = s["removed"] / s["total"] if s["total"] else 0.0
        avg_velocity = (
            statistics.median(s["velocity"]) if s["velocity"] else None
        )
        competition_score = _compute_competition_score(s["active"], removal_rate)
        results.append({
            "token": tok,
            "total_listings": s["total"],
            "active_listings": s["active"],
            "removed_listings": s["removed"],
            "removal_rate": round(removal_rate, 3),
            "avg_velocity_days": round(avg_velocity, 1) if avg_velocity is not None else None,
            "competition_score": competition_score,
        })

    results.sort(key=lambda x: -x["competition_score"])
    return results


def persist(rows: list[dict]) -> int:
    init_db()
    computed_at = now_iso()
    with connect() as conn:
        conn.execute("DELETE FROM market_density")
        for r in rows:
            conn.execute(
                """
                INSERT INTO market_density
                  (token, total_listings, active_listings, removed_listings,
                   removal_rate, avg_velocity_days, competition_score, computed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (r["token"], r["total_listings"], r["active_listings"],
                 r["removed_listings"], r["removal_rate"],
                 r["avg_velocity_days"], r["competition_score"], computed_at),
            )
    log.info(kv(event="market_density_persisted", tokens=len(rows)))
    return len(rows)


def run(min_count: int = MIN_COUNT, dry_run: bool = False) -> dict:
    rows = compute(min_count=min_count)
    if not dry_run:
        persist(rows)
    return {"tokens_analyzed": len(rows)}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--min-count", type=int, default=MIN_COUNT)
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    rows = compute(min_count=args.min_count)
    if not args.dry_run:
        persist(rows)

    print(f"{'token':<22s} {'total':>7s} {'active':>7s} {'removed':>8s} "
          f"{'rem_rate':>9s} {'vel_d':>7s} {'comp':>5s}")
    print("-" * 75)
    for r in rows[:args.top]:
        vel = f"{r['avg_velocity_days']:.1f}" if r['avg_velocity_days'] is not None else "-"
        print(f"{r['token']:<22s} {r['total_listings']:>7d} "
              f"{r['active_listings']:>7d} {r['removed_listings']:>8d} "
              f"{r['removal_rate']:>9.3f} {vel:>7s} "
              f"{r['competition_score']:>5d}")
    print(f"\ntokens_analyzed: {len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
