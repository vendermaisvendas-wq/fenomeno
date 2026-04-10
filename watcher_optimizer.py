"""
Otimizador de watchers populares.

Quando vários watchers compartilham (keyword, region), faz sentido fazer
discovery uma vez só e deixar cada watcher individual processar os
resultados (cada um tem seu próprio filtro de preço).

Estratégia: pre-warm do `discovery_cache`. O cache TTL já é a infraestrutura
de compartilhamento — basta rodar `marketplace_discovery_engine.discover_for`
para cada grupo popular antes da rodada de watchers, e os monitor_watch
subsequentes vão hit no cache.

Funções:
    find_popular_groups(min_users=2)  → identifica grupos
    prewarm_groups(min_users=2)       → roda discovery para cada um (alimenta cache)
    summary()                          → contagem por grupo

Uso:
    python watcher_optimizer.py
    python watcher_optimizer.py --min-users 3
    python watcher_optimizer.py --summary
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from db import connect, init_db
from logging_setup import get_logger, kv

log = get_logger("watcher_optimizer")

DEFAULT_MIN_USERS = 2


@dataclass
class WatcherGroup:
    keyword: str
    region: str | None
    watchers: int
    watch_ids: list[int]


def find_popular_groups(min_users: int = DEFAULT_MIN_USERS) -> list[WatcherGroup]:
    """Agrupa watchers ativos por (keyword.lower(), region.lower()).

    Faz o GROUP BY em Python e não no SQL: o `LOWER()` do SQLite só lida
    com ASCII, então 'AÇAÍ' e 'açaí' caem em buckets diferentes via SQL.
    `str.lower()` do Python normaliza Unicode corretamente.
    """
    init_db()
    with connect() as conn:
        rows = conn.execute(
            "SELECT watch_id, keyword, region FROM watchers WHERE is_active = 1"
        ).fetchall()

    buckets: dict[tuple[str, str], list[dict]] = {}
    for r in rows:
        key = (
            (r["keyword"] or "").lower(),
            (r["region"] or "").lower(),
        )
        buckets.setdefault(key, []).append({
            "watch_id": r["watch_id"],
            "keyword": r["keyword"],
            "region": r["region"],
        })

    out: list[WatcherGroup] = []
    for items in buckets.values():
        if len(items) < min_users:
            continue
        first = items[0]
        out.append(WatcherGroup(
            keyword=first["keyword"],
            region=first["region"],
            watchers=len(items),
            watch_ids=sorted(it["watch_id"] for it in items),
        ))
    out.sort(key=lambda g: -g.watchers)
    return out


def prewarm_groups(min_users: int = DEFAULT_MIN_USERS) -> dict:
    """Para cada grupo popular, roda `marketplace_discovery_engine.discover_for`,
    que alimenta o `discovery_cache`. Não toca em listings nem em watcher_results
    diretamente — só populua o cache, que monitor_watch consome depois."""
    groups = find_popular_groups(min_users)
    if not groups:
        log.info(kv(event="no_popular_groups"))
        return {"groups": 0, "queries_run": 0, "cache_hits": 0,
                "unique_hits": 0, "watchers_covered": 0}

    from marketplace_discovery_engine import discover_for

    total_queries = 0
    total_cache_hits = 0
    total_unique = 0
    watchers_covered = 0

    for g in groups:
        try:
            result = discover_for(keyword=g.keyword, region=g.region)
            total_queries += result.get("queries_run", 0)
            total_cache_hits += result.get("cache_hits", 0)
            total_unique += result.get("total_unique_hits", 0)
            watchers_covered += g.watchers
            log.info(kv(event="group_prewarmed",
                        keyword=g.keyword, region=g.region,
                        watchers=g.watchers,
                        unique_hits=result.get("total_unique_hits", 0)))
        except Exception as e:  # noqa: BLE001
            log.error(kv(event="prewarm_failed",
                         keyword=g.keyword, region=g.region,
                         error=type(e).__name__))

    return {
        "groups": len(groups),
        "queries_run": total_queries,
        "cache_hits": total_cache_hits,
        "unique_hits": total_unique,
        "watchers_covered": watchers_covered,
    }


def summary(min_users: int = DEFAULT_MIN_USERS) -> None:
    groups = find_popular_groups(min_users)
    if not groups:
        print(f"(no groups with >= {min_users} watchers)")
        return
    print(f"{'#watchers':>9s}  {'keyword':<25s} {'region':<25s}  watch_ids")
    print("-" * 80)
    for g in groups:
        print(f"{g.watchers:>9d}  {g.keyword[:25]:<25s} {(g.region or '-')[:25]:<25s}  "
              f"{','.join(map(str, g.watch_ids))}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--min-users", type=int, default=DEFAULT_MIN_USERS)
    ap.add_argument("--summary", action="store_true")
    args = ap.parse_args()

    if args.summary:
        summary(args.min_users)
        return 0

    result = prewarm_groups(args.min_users)
    for k, v in result.items():
        print(f"  {k:20s} {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
