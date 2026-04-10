"""
Clustering de anúncios semelhantes (mais loose que duplicate_detector).

Algoritmo: DBSCAN-like com conectividade via Jaccard sobre tokens. Dois
itens são "vizinhos" quando jaccard ≥ EPS_JACCARD e (se ambos tiverem)
compartilham a mesma marca e anos próximos. Conectividade propaga via
Union-Find, formando cluster_id por componente conectada.

Diferença do duplicate_detector:
  listing_cluster   : "hilux 2013 diesel srv" ~ "hilux 2014 diesel sr" ✓
                      (mesma família de produto; jaccard ≥ 0.5)
  duplicate_detector: mesmo anúncio rebuscado em cidades ~próximas
                      (jaccard ≥ 0.7, preço ±10%, mesma cidade)

Usa ComparablesIndex do market_value para não ser O(n²) puro: só compara
itens que compartilham ≥ 1 token (via índice invertido).

Salva em `listings.cluster_id`.

Uso:
    python listing_cluster.py
    python listing_cluster.py --eps 0.4 --dry-run
"""

from __future__ import annotations

import argparse

from db import connect, init_db
from duplicate_detector import UnionFind
from logging_setup import get_logger, kv
from market_value import ComparablesIndex, _load_priced_items
from title_normalizer import jaccard

log = get_logger("listing_cluster")

DEFAULT_EPS_JACCARD = 0.5
YEAR_TOLERANCE = 2


def _connectable(a, b, eps: float) -> bool:
    """Dois items podem ser vizinhos? (brand/year compatível + jaccard ≥ eps)"""
    # Marcas conflitantes → não
    if a.brand and b.brand and a.brand != b.brand:
        return False
    # Anos muito distantes → não (só aplica quando ambos têm ano)
    if a.year is not None and b.year is not None:
        if abs(a.year - b.year) > YEAR_TOLERANCE:
            return False
    return jaccard(a.tokens, b.tokens) >= eps


def cluster_all(eps: float = DEFAULT_EPS_JACCARD, dry_run: bool = False) -> dict:
    init_db()
    with connect() as conn:
        items = _load_priced_items(conn, exclude_outliers=False)
        index = ComparablesIndex(items)

        uf = UnionFind()
        for it in items:
            uf.add(it.id)

        edges = 0
        for it in items:
            # Candidatos = qualquer item que compartilhe ≥ 1 token (invindex)
            candidates: set[str] = set()
            for tok in it.tokens:
                candidates |= index.by_token.get(tok, set())
            candidates.discard(it.id)

            for cid in candidates:
                other = index.items_by_id[cid]
                if _connectable(it, other, eps):
                    uf.union(it.id, cid)
                    edges += 1

        # Assign cluster_id sequencial por componente
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
                    "UPDATE listings SET cluster_id = ? WHERE id = ?",
                    (cid, it.id),
                )

        multi = sum(1 for s in cluster_sizes.values() if s > 1)
        biggest = max(cluster_sizes.values()) if cluster_sizes else 0

        result = {
            "items": len(items),
            "edges_considered": edges,
            "clusters": len(cluster_sizes),
            "multi_member_clusters": multi,
            "biggest_cluster_size": biggest,
            "eps": eps,
        }
        log.info(kv(event="listings_clustered", **result))
        return result


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--eps", type=float, default=DEFAULT_EPS_JACCARD)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    result = cluster_all(eps=args.eps, dry_run=args.dry_run)
    for k, v in result.items():
        print(f"  {k:25s} {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
