"""
Análise de velocidade de venda: tempo médio até o anúncio ser removido.

Calcula sobre `listings.removed_at - listings.first_seen_at` (só os já
removidos). Métricas globais + por-token (permite ver quais itens têm
maior liquidez).

Não tem ground truth de "foi vendido" — "removido" pode ser:
  (a) vendido → bom sinal de liquidez
  (b) cancelado pelo vendedor → ruído
  (c) apagado por moderação → outro ruído

Na prática, o sinal ainda é útil em agregado: itens da categoria X
ficam em média N dias antes de sumir.

Uso:
    python sales_velocity.py                 # stats globais + top 20 tokens
    python sales_velocity.py --token iphone  # só este token
    python sales_velocity.py --json
"""

from __future__ import annotations

import argparse
import json
import statistics
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from db import connect, init_db
from logging_setup import get_logger, kv
from title_normalizer import tokens

log = get_logger("velocity")

MIN_GROUP = 3


@dataclass
class VelocityStats:
    scope: str
    removed_count: int
    mean_days: float
    median_days: float
    min_days: float
    max_days: float
    p25_days: float
    p75_days: float


def _parse_dt(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _days_between(first_seen: str, removed: str) -> float:
    d = _parse_dt(removed) - _parse_dt(first_seen)
    return d.total_seconds() / 86400.0


def _compute(days_list: list[float], scope: str) -> VelocityStats | None:
    if len(days_list) < MIN_GROUP:
        return None
    sorted_d = sorted(days_list)
    # percentil inline para não criar dependência circular com market_value
    def _pct(p: float) -> float:
        if len(sorted_d) == 1:
            return sorted_d[0]
        k = (len(sorted_d) - 1) * (p / 100.0)
        f = int(k)
        c = min(f + 1, len(sorted_d) - 1)
        if f == c:
            return sorted_d[f]
        return sorted_d[f] + (sorted_d[c] - sorted_d[f]) * (k - f)

    return VelocityStats(
        scope=scope,
        removed_count=len(sorted_d),
        mean_days=round(statistics.fmean(sorted_d), 2),
        median_days=round(statistics.median(sorted_d), 2),
        min_days=round(sorted_d[0], 2),
        max_days=round(sorted_d[-1], 2),
        p25_days=round(_pct(25), 2),
        p75_days=round(_pct(75), 2),
    )


def compute_global() -> VelocityStats | None:
    init_db()
    with connect() as conn:
        rows = conn.execute(
            "SELECT first_seen_at, removed_at FROM listings "
            "WHERE is_removed = 1 AND removed_at IS NOT NULL"
        ).fetchall()
    days = [_days_between(r["first_seen_at"], r["removed_at"]) for r in rows]
    return _compute(days, scope="global")


def compute_by_token(limit: int = 20) -> list[VelocityStats]:
    init_db()
    with connect() as conn:
        rows = conn.execute(
            "SELECT current_title, first_seen_at, removed_at FROM listings "
            "WHERE is_removed = 1 AND removed_at IS NOT NULL "
            "AND current_title IS NOT NULL"
        ).fetchall()

    buckets: dict[str, list[float]] = {}
    for r in rows:
        days = _days_between(r["first_seen_at"], r["removed_at"])
        for tok in tokens(r["current_title"]):
            buckets.setdefault(tok, []).append(days)

    results: list[VelocityStats] = []
    for tok, days_list in buckets.items():
        stats = _compute(days_list, scope=f"token:{tok}")
        if stats:
            results.append(stats)

    # Ordenar por contagem desc, depois por median asc (vende rápido)
    results.sort(key=lambda s: (-s.removed_count, s.median_days))
    return results[:limit]


def compute_for_token(token: str) -> VelocityStats | None:
    init_db()
    token_l = token.lower().strip()
    with connect() as conn:
        rows = conn.execute(
            "SELECT current_title, first_seen_at, removed_at FROM listings "
            "WHERE is_removed = 1 AND removed_at IS NOT NULL "
            "AND LOWER(COALESCE(current_title, '')) LIKE ?",
            (f"%{token_l}%",),
        ).fetchall()
    days = [_days_between(r["first_seen_at"], r["removed_at"]) for r in rows]
    return _compute(days, scope=f"token:{token_l}")


def _print(stats: VelocityStats) -> None:
    print(f"  {stats.scope:30s}  n={stats.removed_count:4d}  "
          f"mean={stats.mean_days:6.1f}d  median={stats.median_days:6.1f}d  "
          f"p25={stats.p25_days:6.1f}d  p75={stats.p75_days:6.1f}d")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--token", help="analisa apenas este token")
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if args.token:
        stats = compute_for_token(args.token)
        if stats is None:
            print("sem dados suficientes para este token")
            return 1
        if args.json:
            print(json.dumps(asdict(stats), indent=2))
        else:
            _print(stats)
        return 0

    glob = compute_global()
    per_token = compute_by_token(args.limit)

    if args.json:
        print(json.dumps({
            "global": asdict(glob) if glob else None,
            "per_token": [asdict(s) for s in per_token],
        }, indent=2))
        return 0

    if glob is None:
        print("sem histórico suficiente de anúncios removidos")
        return 1
    print("=== Global ===")
    _print(glob)
    print(f"\n=== Top {args.limit} por volume ===")
    for s in per_token:
        _print(s)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
