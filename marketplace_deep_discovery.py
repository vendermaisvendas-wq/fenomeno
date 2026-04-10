"""
Deep discovery: BFS sobre o discovery_graph.

Estende `marketplace_discovery_engine` com expansão recursiva: para cada
listing descoberto, tira `derive_queries()` do título e adiciona como
queries filhas. O processo continua até atingir `max_depth` ou `max_total_queries`.

Cada aresta (parent_query → child_query) é registrada na tabela
`discovery_graph`. Permite reconstruir a lineage: "essa keyword desconhecida
veio porque encontramos aquele anúncio que veio de outra query".

Aplicação típica:
    deep_discover_for("iphone", region="Araçatuba", max_depth=2)

Comportamento:
    - depth=0: query seed (`iphone`)
    - depth=1: queries derivadas dos listings encontrados em depth=0
    - depth=2: queries derivadas dos listings encontrados em depth=1
    - ...
    - cada query passa pelo discovery_cache TTL
    - dedup global por listing_id

Garantias:
    - Não explode: limita por max_total_queries (default 30)
    - Não redunda: discovery_graph UNIQUE constraint impede mesmo edge 2x
    - Não recursa em loop: child_query já vista é pulada

Uso:
    python marketplace_deep_discovery.py iphone --region Araçatuba
    python marketplace_deep_discovery.py "playstation 5" --max-depth 2 --max-queries 20
    python marketplace_deep_discovery.py iphone --json
    python marketplace_deep_discovery.py --graph-summary
"""

from __future__ import annotations

import argparse
import json
from collections import deque

from db import connect, init_db, now_iso
from logging_setup import get_logger, kv

log = get_logger("deep_discovery")

DEFAULT_MAX_DEPTH = 2
DEFAULT_MAX_QUERIES = 30


# --- discovery_graph helpers ---------------------------------------------

def add_edge(
    parent_query: str | None,
    child_query: str,
    source_listing_id: str | None,
    depth: int,
) -> bool:
    """INSERT OR IGNORE — retorna True se adicionou.

    parent_query=None é normalizado para '' no storage. Razão: SQLite UNIQUE
    trata NULL como sempre distinto de NULL, então (NULL, 'iphone') aceitaria
    múltiplas linhas. Empty string é valor concreto e dedup funciona.
    """
    init_db()
    parent_norm = parent_query if parent_query is not None else ""
    with connect() as conn:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO discovery_graph
              (parent_query, child_query, source_listing_id, depth, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (parent_norm, child_query, source_listing_id, depth, now_iso()),
        )
        return cur.rowcount > 0


def all_known_queries() -> set[str]:
    init_db()
    with connect() as conn:
        rows = conn.execute(
            "SELECT DISTINCT child_query FROM discovery_graph"
        ).fetchall()
    return {r["child_query"] for r in rows}


def graph_summary() -> dict:
    init_db()
    with connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM discovery_graph").fetchone()[0]
        # Roots = parent vazio ou NULL (legacy). Empty string é o canonico v10.
        roots = conn.execute(
            "SELECT COUNT(*) FROM discovery_graph "
            "WHERE parent_query IS NULL OR parent_query = ''"
        ).fetchone()[0]
        max_depth = conn.execute(
            "SELECT MAX(depth) FROM discovery_graph"
        ).fetchone()[0]
        by_depth = conn.execute(
            "SELECT depth, COUNT(*) AS n FROM discovery_graph GROUP BY depth ORDER BY depth"
        ).fetchall()
    return {
        "total_edges": total,
        "roots": roots,
        "max_depth": max_depth or 0,
        "by_depth": [dict(r) for r in by_depth],
    }


def edges_from(parent: str | None, limit: int = 50) -> list[dict]:
    init_db()
    with connect() as conn:
        if parent is None:
            # Roots = NULL (legacy) ou empty string (v10 canonico)
            rows = conn.execute(
                "SELECT * FROM discovery_graph "
                "WHERE parent_query IS NULL OR parent_query = '' "
                "ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM discovery_graph WHERE parent_query = ? "
                "ORDER BY id DESC LIMIT ?",
                (parent, limit),
            ).fetchall()
    return [dict(r) for r in rows]


# --- BFS principal --------------------------------------------------------

def deep_discover_for(
    keyword: str,
    region: str | None = None,
    max_depth: int = DEFAULT_MAX_DEPTH,
    max_total_queries: int = DEFAULT_MAX_QUERIES,
    use_cache: bool = True,
) -> dict:
    """BFS partindo de `keyword`. Cada listing descoberto vira fonte de
    queries derivadas (depth + 1). Termina quando atingir max_depth OU
    max_total_queries."""
    from marketplace_discovery_engine import discover_for
    from related_listing_finder import derive_queries

    log.info(kv(event="deep_discovery_start",
                keyword=keyword, region=region, max_depth=max_depth))

    visited_queries: set[str] = set()
    seen_listing_ids: set[str] = set()
    all_listings: list[dict] = []
    queries_run = 0
    cache_hits_total = 0
    edges_added = 0

    # BFS queue: (query, parent_query, depth, source_listing_id)
    queue: deque[tuple[str, str | None, int, str | None]] = deque()
    queue.append((keyword.lower(), None, 0, None))

    while queue and queries_run < max_total_queries:
        query, parent, depth, source = queue.popleft()
        if query in visited_queries:
            continue
        visited_queries.add(query)

        # registra a aresta
        if add_edge(parent, query, source, depth):
            edges_added += 1

        try:
            result = discover_for(
                keyword=query, region=region,
                max_pages=2, use_cache=use_cache,
            )
        except Exception as e:  # noqa: BLE001
            log.error(kv(event="deep_query_failed",
                         query=query, error=type(e).__name__))
            continue

        queries_run += result.get("queries_run", 0)
        cache_hits_total += result.get("cache_hits", 0)

        # Para cada listing novo, eventualmente expande
        for h in result.get("hits", []):
            iid = h.get("item_id")
            if not iid or iid in seen_listing_ids:
                continue
            seen_listing_ids.add(iid)
            all_listings.append(h)

            if depth >= max_depth:
                continue

            # gera queries derivadas
            derived = derive_queries(h.get("title"))
            for child in derived:
                if child not in visited_queries:
                    queue.append((child, query, depth + 1, iid))

    summary = {
        "seed_keyword": keyword,
        "region": region,
        "max_depth": max_depth,
        "queries_visited": len(visited_queries),
        "queries_run_ddg": queries_run,
        "cache_hits_total": cache_hits_total,
        "unique_listings": len(all_listings),
        "graph_edges_added": edges_added,
    }
    log.info(kv(event="deep_discovery_done", **summary))
    return {**summary, "listings": all_listings}


# --- CLI ------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd")

    sp = sub.add_parser("run", help="executa deep discovery")
    sp.add_argument("keyword", nargs="?")
    sp.add_argument("--region")
    sp.add_argument("--max-depth", type=int, default=DEFAULT_MAX_DEPTH)
    sp.add_argument("--max-queries", type=int, default=DEFAULT_MAX_QUERIES)
    sp.add_argument("--no-cache", action="store_true")
    sp.add_argument("--json", action="store_true")

    sub.add_parser("graph", help="resumo do discovery_graph")

    # default cmd: run (compatibilidade com chamada simples)
    args, extras = ap.parse_known_args()

    if args.cmd is None and extras:
        args.cmd = "run"
        args.keyword = extras[0]

    if args.cmd == "graph":
        s = graph_summary()
        for k, v in s.items():
            print(f"  {k:20s} {v}")
        return 0

    if args.cmd == "run":
        if not args.keyword:
            ap.error("forneça a keyword")
        result = deep_discover_for(
            keyword=args.keyword,
            region=args.region,
            max_depth=args.max_depth,
            max_total_queries=args.max_queries,
            use_cache=not args.no_cache,
        )
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0
        for k, v in result.items():
            if k == "listings":
                continue
            print(f"  {k:25s} {v}")
        print(f"\n  first 10 listings:")
        for h in result["listings"][:10]:
            print(f"    {h['item_id']}  {(h.get('title') or '')[:60]}")
        return 0

    ap.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
