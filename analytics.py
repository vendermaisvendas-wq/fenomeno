"""
Analytics sobre os listings coletados.

Lê o estado atual de `listings` no banco e agrupa por palavras-chave que
aparecem no título (case-insensitive, substring). Para cada grupo, calcula
contagem, média, mínimo, máximo, mediana e desvio padrão de preço.

Uso:
    python analytics.py hilux cg iphone
    python analytics.py --all                     # usa todos os listings sem agrupar
    python analytics.py --json hilux cg           # saída JSON para pipe
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from dataclasses import asdict, dataclass

from db import connect


@dataclass
class Stats:
    keyword: str
    count: int
    mean: float | None
    median: float | None
    stdev: float | None
    minimum: float | None
    maximum: float | None
    currency: str | None


def _to_float(price: str | None) -> float | None:
    """Retido por compatibilidade. Delega para price_normalizer.parse()."""
    from price_normalizer import parse
    return parse(price)


def load_listings() -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT id, current_title AS title, current_price AS price,
                   current_currency AS currency, is_removed
              FROM listings
             WHERE is_removed = 0
               AND current_title IS NOT NULL
            """
        ).fetchall()
    return [dict(r) for r in rows]


def compute_stats(listings: list[dict], keyword: str) -> Stats:
    kw = keyword.lower().strip()
    matching = [l for l in listings if kw in (l["title"] or "").lower()]
    prices = [p for p in (_to_float(l["price"]) for l in matching) if p is not None]
    currencies = {l["currency"] for l in matching if l["currency"]}
    currency = next(iter(currencies)) if len(currencies) == 1 else None

    if not prices:
        return Stats(keyword, count=len(matching), mean=None, median=None,
                     stdev=None, minimum=None, maximum=None, currency=currency)

    return Stats(
        keyword=keyword,
        count=len(matching),
        mean=round(statistics.fmean(prices), 2),
        median=round(statistics.median(prices), 2),
        stdev=round(statistics.pstdev(prices), 2) if len(prices) > 1 else 0.0,
        minimum=min(prices),
        maximum=max(prices),
        currency=currency,
    )


def analyze(keywords: list[str]) -> list[Stats]:
    listings = load_listings()
    return [compute_stats(listings, kw) for kw in keywords]


def print_table(stats: list[Stats]) -> None:
    header = f"{'keyword':<20} {'n':>5} {'mean':>12} {'median':>12} {'stdev':>12} {'min':>10} {'max':>10} cur"
    print(header)
    print("-" * len(header))
    for s in stats:
        def fmt(v):
            return f"{v:,.2f}" if isinstance(v, (int, float)) else "-"
        print(
            f"{s.keyword:<20} {s.count:>5} "
            f"{fmt(s.mean):>12} {fmt(s.median):>12} {fmt(s.stdev):>12} "
            f"{fmt(s.minimum):>10} {fmt(s.maximum):>10} {s.currency or '-'}"
        )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("keywords", nargs="*", help="palavras-chave para agrupar")
    ap.add_argument("--all", action="store_true",
                    help="estatística global (ignora keywords)")
    ap.add_argument("--json", action="store_true", help="saída JSON")
    args = ap.parse_args()

    if args.all:
        listings = load_listings()
        prices = [p for p in (_to_float(l["price"]) for l in listings) if p is not None]
        print(f"total ativos com título: {len(listings)}")
        print(f"com preço parseável:     {len(prices)}")
        if prices:
            print(f"mean:    {statistics.fmean(prices):,.2f}")
            print(f"median:  {statistics.median(prices):,.2f}")
            print(f"min:     {min(prices):,.2f}")
            print(f"max:     {max(prices):,.2f}")
            if len(prices) > 1:
                print(f"stdev:   {statistics.pstdev(prices):,.2f}")
        return 0

    if not args.keywords:
        print("erro: forneça keywords ou --all", file=sys.stderr)
        return 2

    stats = analyze(args.keywords)
    if args.json:
        print(json.dumps([asdict(s) for s in stats], ensure_ascii=False, indent=2))
    else:
        print_table(stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
