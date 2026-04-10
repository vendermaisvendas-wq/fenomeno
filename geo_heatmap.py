"""
Geração de dataset para heatmap geográfico.

Consome `geo_coverage` (já persistido) e produz payloads prontos para
consumo no dashboard (Chart.js bar charts) ou export JSON.

Não computa nada novo — é uma camada de apresentação sobre o que
`geo_coverage.py` já calculou. Para refresh, rode `geo_coverage.py` antes.

Funções:
    top_cities_by_volume(limit)    ordenação por active_count desc
    top_cities_by_discount(limit)  por avg_discount desc
    by_state()                      agregação por UF (n cidades, n ativos)
    heatmap_dataset()               dict com tudo necessário para o template

Uso:
    python geo_heatmap.py                    # imprime resumo
    python geo_heatmap.py --top 20
    python geo_heatmap.py --json > heatmap.json
    python geo_heatmap.py --by-state
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass

from db import connect, init_db
from logging_setup import get_logger

log = get_logger("geo_heatmap")


@dataclass
class CityHeat:
    city: str
    state: str | None
    listings_count: int
    active_count: int
    avg_price: float | None
    avg_discount: float | None
    coverage_score: int


@dataclass
class StateAgg:
    state: str
    cities: int
    listings_count: int
    active_count: int
    avg_discount: float | None


def _load_cities() -> list[CityHeat]:
    init_db()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT city, state, listings_count, active_count, avg_price,
                   avg_discount, coverage_score
              FROM geo_coverage
             ORDER BY active_count DESC
            """
        ).fetchall()
    return [CityHeat(**dict(r)) for r in rows]


def top_cities_by_volume(limit: int = 25) -> list[CityHeat]:
    return _load_cities()[:limit]


def top_cities_by_discount(limit: int = 25) -> list[CityHeat]:
    cities = [c for c in _load_cities() if c.avg_discount is not None]
    cities.sort(key=lambda c: -c.avg_discount)
    return cities[:limit]


def by_state() -> list[StateAgg]:
    cities = _load_cities()
    buckets: dict[str, list[CityHeat]] = {}
    for c in cities:
        if c.state:
            buckets.setdefault(c.state, []).append(c)

    results: list[StateAgg] = []
    for state, items in buckets.items():
        discounts = [i.avg_discount for i in items if i.avg_discount is not None]
        results.append(StateAgg(
            state=state,
            cities=len(items),
            listings_count=sum(i.listings_count for i in items),
            active_count=sum(i.active_count for i in items),
            avg_discount=round(sum(discounts) / len(discounts), 2) if discounts else None,
        ))
    results.sort(key=lambda s: -s.active_count)
    return results


def heatmap_dataset(limit: int = 25) -> dict:
    vol = top_cities_by_volume(limit)
    disc = top_cities_by_discount(limit)
    states = by_state()
    return {
        "top_by_volume": [asdict(c) for c in vol],
        "top_by_discount": [asdict(c) for c in disc],
        "by_state": [asdict(s) for s in states],
    }


def _print_cities(title: str, rows: list[CityHeat]) -> None:
    print(f"\n=== {title} ===")
    print(f"{'city':<30s} {'state':5s} {'active':>8s} {'avg_price':>12s} "
          f"{'avg_disc':>10s} {'score':>6s}")
    print("-" * 75)
    for c in rows:
        ap_s = f"{c.avg_price:,.0f}" if c.avg_price else "-"
        ad_s = f"{c.avg_discount:.1f}%" if c.avg_discount is not None else "-"
        print(
            f"{(c.city or '')[:30]:<30s} {(c.state or '-'):5s} "
            f"{c.active_count:>8d} {ap_s:>12s} {ad_s:>10s} "
            f"{c.coverage_score:>6d}"
        )


def _print_states(states: list[StateAgg]) -> None:
    print("\n=== Por estado ===")
    print(f"{'UF':5s} {'cidades':>8s} {'ativos':>8s} {'avg_disc':>10s}")
    print("-" * 40)
    for s in states:
        ad = f"{s.avg_discount:.1f}%" if s.avg_discount is not None else "-"
        print(f"{s.state:5s} {s.cities:>8d} {s.active_count:>8d} {ad:>10s}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--by-state", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if args.json:
        print(json.dumps(heatmap_dataset(args.top), ensure_ascii=False, indent=2))
        return 0

    if args.by_state:
        _print_states(by_state())
        return 0

    _print_cities("Top por volume", top_cities_by_volume(args.top))
    _print_cities("Top por desconto médio", top_cities_by_discount(args.top))
    _print_states(by_state()[:15])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
