"""
Detecta anúncios possivelmente duplicados e atribui `duplicate_group_id`
em `listings` (v5: coluna separada de `cluster_id`, que agora pertence ao
listing_cluster.py).

Critério de duplicata (todos precisam ser verdadeiros):
  - Jaccard(tokens(títuloA), tokens(títuloB)) >= 0.7
  - abs(precoA - precoB) / min(precoA, precoB) <= 0.10  (≤10% diff)
  - mesma cidade (primeiro token de location_text)     se ambos tiverem

Algoritmo: Union-Find O(n²) sobre listings ativos. Aceitável até ~5k
listings; para escala maior precisaria de blocking por marca/cidade antes.

Uso:
    python duplicate_detector.py
    python duplicate_detector.py --dry-run
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from analytics import _to_float
from db import connect, init_db
from logging_setup import get_logger, kv
from title_normalizer import jaccard, tokens

log = get_logger("duplicates")

TITLE_SIM_THRESHOLD = 0.7
PRICE_TOLERANCE = 0.10


@dataclass
class Item:
    id: str
    tokens: set[str]
    price: float | None
    city: str | None


class UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def add(self, x: str) -> None:
        if x not in self.parent:
            self.parent[x] = x

    def find(self, x: str) -> str:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]  # path compression
            x = self.parent[x]
        return x

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def _city_of(location_text: str | None) -> str | None:
    if not location_text:
        return None
    return location_text.split(",")[0].strip().lower() or None


def is_similar(a: Item, b: Item) -> bool:
    if jaccard(a.tokens, b.tokens) < TITLE_SIM_THRESHOLD:
        return False
    if a.price is not None and b.price is not None:
        pmin, pmax = sorted((a.price, b.price))
        if pmin <= 0:
            return False
        if (pmax - pmin) / pmin > PRICE_TOLERANCE:
            return False
    if a.city and b.city and a.city != b.city:
        return False
    return True


def _load_items(conn) -> list[Item]:
    rows = conn.execute(
        "SELECT id, current_title, current_price, current_location "
        "FROM listings WHERE is_removed = 0 AND current_title IS NOT NULL"
    ).fetchall()
    out: list[Item] = []
    for r in rows:
        out.append(Item(
            id=r["id"],
            tokens=tokens(r["current_title"]),
            price=_to_float(r["current_price"]),
            city=_city_of(r["current_location"]),
        ))
    return out


def cluster_all(dry_run: bool = False) -> dict:
    init_db()
    with connect() as conn:
        items = _load_items(conn)

        uf = UnionFind()
        for it in items:
            uf.add(it.id)

        comparisons = 0
        for i in range(len(items)):
            a = items[i]
            for j in range(i + 1, len(items)):
                b = items[j]
                comparisons += 1
                if is_similar(a, b):
                    uf.union(a.id, b.id)

        # Atribui cluster_id sequencial começando em 1
        root_to_cid: dict[str, int] = {}
        next_cid = 1
        cluster_sizes: dict[int, int] = {}

        for it in items:
            root = uf.find(it.id)
            if root not in root_to_cid:
                root_to_cid[root] = next_cid
                next_cid += 1
            cid = root_to_cid[root]
            cluster_sizes[cid] = cluster_sizes.get(cid, 0) + 1
            if not dry_run:
                conn.execute(
                    "UPDATE listings SET duplicate_group_id = ? WHERE id = ?",
                    (cid, it.id),
                )

        multi = [cid for cid, sz in cluster_sizes.items() if sz > 1]
        duplicate_count = sum(cluster_sizes[cid] for cid in multi)

        result = {
            "listings": len(items),
            "comparisons": comparisons,
            "clusters": len(cluster_sizes),
            "multi_member_clusters": len(multi),
            "listings_in_multi_clusters": duplicate_count,
        }
        log.info(kv(event="duplicates_clustered", **result))
        return result


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    result = cluster_all(dry_run=args.dry_run)
    for k, v in result.items():
        print(f"  {k:35s} {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
