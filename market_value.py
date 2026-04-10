"""
Motor de valor de mercado.

Para cada listing ativo com preço parseável, encontra "comparáveis" no
próprio banco e estima:

    estimated_market_value = mediana dos preços dos comparáveis
    discount_percentage    = (mediana − preço) / mediana * 100

Estratégia de busca de comparáveis (cai em cascata até achar ≥ MIN_COMP):

  1. mesma marca + mesmo ano      (hilux 2013 vs hilux 2013)
  2. mesma marca, qualquer ano    (hilux 2013 vs hilux)
  3. Jaccard ≥ 0.5 sobre tokens   (fallback para itens sem marca reconhecida)

Também expõe `group_stats(token)` que calcula mean/median/p25/p75/n/σ de
listings cujo título contém um token específico — usado pelo heatmap e
pelo new_listing_detector.

Uso:
    python market_value.py            # recomputa tudo e imprime sumário
    python market_value.py --dry-run  # não grava
"""

from __future__ import annotations

import argparse
import statistics
from dataclasses import dataclass

from db import connect, init_db
from logging_setup import get_logger, kv
from price_normalizer import parse as parse_price
from title_normalizer import extract_brand, extract_year, jaccard, tokens

log = get_logger("market_value")

MIN_COMPARABLES = 3
JACCARD_FALLBACK = 0.5


@dataclass
class PricedItem:
    id: str
    price: float
    tokens: set[str]
    brand: str | None
    year: int | None
    title: str


@dataclass
class GroupStats:
    count: int
    mean: float
    median: float
    p25: float
    p75: float
    stdev: float


# --- percentis --------------------------------------------------------------

def percentile(sorted_prices: list[float], p: float) -> float:
    """Percentil linear (tipo 7 do R). `sorted_prices` deve estar ordenado asc."""
    if not sorted_prices:
        raise ValueError("empty prices")
    if len(sorted_prices) == 1:
        return sorted_prices[0]
    k = (len(sorted_prices) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_prices) - 1)
    if f == c:
        return sorted_prices[f]
    return sorted_prices[f] + (sorted_prices[c] - sorted_prices[f]) * (k - f)


def compute_group_stats(prices: list[float]) -> GroupStats | None:
    if len(prices) < 2:
        return None
    sorted_p = sorted(prices)
    return GroupStats(
        count=len(sorted_p),
        mean=statistics.fmean(sorted_p),
        median=statistics.median(sorted_p),
        p25=percentile(sorted_p, 25),
        p75=percentile(sorted_p, 75),
        stdev=statistics.pstdev(sorted_p),
    )


# --- carga --------------------------------------------------------------

def _load_priced_items(conn, exclude_outliers: bool = False) -> list[PricedItem]:
    where = "WHERE is_removed = 0 AND current_title IS NOT NULL"
    if exclude_outliers:
        where += " AND COALESCE(price_outlier, 0) = 0"
    rows = conn.execute(
        f"SELECT id, current_title, current_price FROM listings {where}"
    ).fetchall()
    items: list[PricedItem] = []
    for r in rows:
        price = parse_price(r["current_price"])
        if price is None or price <= 0:
            continue
        title = r["current_title"] or ""
        items.append(PricedItem(
            id=r["id"],
            price=price,
            tokens=tokens(title),
            brand=extract_brand(title),
            year=extract_year(title),
            title=title,
        ))
    return items


# --- ComparablesIndex: índice invertido para escala ---------------------

class ComparablesIndex:
    """Pré-computa índices sobre um pool de PricedItems para consulta O(1)
    ao invés de O(n) varrendo a lista em cada chamada.

    Estruturas:
      items_by_id      : id → PricedItem
      by_brand         : brand → list[PricedItem]
      by_brand_year    : (brand, year) → list[PricedItem]
      by_token         : token → set[id]   (índice invertido)
      _cache_comps     : id → list[PricedItem]   (memoização por run)

    Para N=1000 com ~5 tokens por título, o fallback Jaccard agora itera
    sobre O(candidates ∩ tokens(item)) ao invés de O(N).
    """

    def __init__(self, items: list[PricedItem]) -> None:
        self.items_by_id: dict[str, PricedItem] = {it.id: it for it in items}
        self.by_brand: dict[str, list[PricedItem]] = {}
        self.by_brand_year: dict[tuple[str, int], list[PricedItem]] = {}
        self.by_token: dict[str, set[str]] = {}

        for it in items:
            if it.brand:
                self.by_brand.setdefault(it.brand, []).append(it)
                if it.year is not None:
                    self.by_brand_year.setdefault((it.brand, it.year), []).append(it)
            for tok in it.tokens:
                self.by_token.setdefault(tok, set()).add(it.id)

        self._cache_comps: dict[str, list[PricedItem]] = {}

    def _exclude_self(self, items: list[PricedItem], self_id: str) -> list[PricedItem]:
        return [p for p in items if p.id != self_id]

    def find_comparables(self, item: PricedItem) -> list[PricedItem]:
        """Cascata: brand+year → brand → jaccard via invindex. Exclui self."""
        if item.id in self._cache_comps:
            return self._cache_comps[item.id]

        result: list[PricedItem] = []

        if item.brand:
            if item.year is not None:
                bucket = self.by_brand_year.get((item.brand, item.year), [])
                same_year = self._exclude_self(bucket, item.id)
                if len(same_year) >= MIN_COMPARABLES:
                    result = same_year
            if not result:
                bucket = self.by_brand.get(item.brand, [])
                same_brand = self._exclude_self(bucket, item.id)
                if len(same_brand) >= MIN_COMPARABLES:
                    result = same_brand

        if not result:
            # Fallback: jaccard sobre candidatos que compartilham ao menos 1 token
            candidates: set[str] = set()
            for tok in item.tokens:
                candidates |= self.by_token.get(tok, set())
            candidates.discard(item.id)
            fallback: list[PricedItem] = []
            for cid in candidates:
                c = self.items_by_id[cid]
                if jaccard(item.tokens, c.tokens) >= JACCARD_FALLBACK:
                    fallback.append(c)
            result = fallback

        self._cache_comps[item.id] = result
        return result


def find_comparables(item: PricedItem, pool: list[PricedItem]) -> list[PricedItem]:
    """Backward-compat: constrói um índice ad-hoc a partir do pool. Para loops
    grandes prefira criar um ComparablesIndex explicitamente e reutilizar."""
    idx = ComparablesIndex(pool)
    return idx.find_comparables(item)


# --- recompute ----------------------------------------------------------

def recompute_all(dry_run: bool = False, exclude_outliers: bool = True) -> dict:
    """Recalcula estimated_market_value + discount_percentage para todos os
    listings ativos. Usa ComparablesIndex para escalar linearmente com N.

    exclude_outliers=True → itens com price_outlier=1 não entram no pool de
    comparáveis (mas ainda recebem estimativa, baseada nos não-outliers).
    """
    init_db()
    with connect() as conn:
        # Pool para comparação exclui outliers (dados "sujos")
        pool = _load_priced_items(conn, exclude_outliers=exclude_outliers)
        # Itens a atualizar: tudo que tem preço, incluindo outliers
        targets = _load_priced_items(conn, exclude_outliers=False)

        index = ComparablesIndex(pool)
        updated = 0
        skipped_no_comp = 0
        total_discount = 0.0

        for item in targets:
            comps = index.find_comparables(item)
            if len(comps) < MIN_COMPARABLES:
                skipped_no_comp += 1
                continue
            prices = sorted(c.price for c in comps)
            median = statistics.median(prices)
            if median <= 0:
                continue
            discount = (median - item.price) / median * 100.0
            total_discount += discount

            if not dry_run:
                conn.execute(
                    "UPDATE listings SET estimated_market_value = ?, "
                    "discount_percentage = ? WHERE id = ?",
                    (median, discount, item.id),
                )
            updated += 1

        result = {
            "pool_size": len(pool),
            "targets": len(targets),
            "updated": updated,
            "skipped_no_comparables": skipped_no_comp,
            "avg_discount_pct": (total_discount / updated) if updated else 0.0,
        }
        log.info(kv(event="market_value_recomputed", **result))
        return result


# --- token-level stats (usado por heatmap + new_listing_detector) --------

def token_group_stats(min_count: int = 5) -> dict[str, GroupStats]:
    """Para cada token comum no universo (≥ min_count listings), computa stats."""
    with connect() as conn:
        items = _load_priced_items(conn)
    buckets: dict[str, list[float]] = {}
    for item in items:
        for tok in item.tokens:
            buckets.setdefault(tok, []).append(item.price)
    out: dict[str, GroupStats] = {}
    for tok, prices in buckets.items():
        if len(prices) < min_count:
            continue
        gs = compute_group_stats(prices)
        if gs:
            out[tok] = gs
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    result = recompute_all(dry_run=args.dry_run)
    print(f"items_total:           {result['items_total']}")
    print(f"updated:               {result['updated']}")
    print(f"skipped_no_comparables: {result['skipped_no_comparables']}")
    print(f"avg_discount_pct:      {result['avg_discount_pct']:.1f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
