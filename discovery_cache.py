"""
Cache TTL para resultados de discovery, persistido em SQLite.

Motivação: a v9 introduz `marketplace_discovery_engine` que roda múltiplas
variações de uma keyword, e `watcher_optimizer` que detecta watchers
populares com a mesma keyword+region. Sem cache, watchers populares fazem
a mesma query DDG repetidamente, desperdiçando rate limit.

Schema (em db.py):
    discovery_cache(query_hash, query_text, region, result_json, expires_at, created_at)

API:
    get(query, region) -> list[dict] | None    # None se cache miss / expirado
    put(query, region, results, ttl=600)       # default 10 min
    cleanup_expired() -> int                    # quantas linhas removidas
    invalidate(query=None, region=None)         # tudo se ambos None

Resultados são serializados como JSON. O cache não armazena objetos
PricedItem ou Listing — armazena `Hit`-shaped dicts (`{url, item_id, title}`)
que são leves e fáceis de serializar.

Uso CLI:
    python discovery_cache.py info
    python discovery_cache.py cleanup
    python discovery_cache.py clear
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timedelta, timezone

from db import connect, init_db, now_iso
from logging_setup import get_logger, kv

log = get_logger("discovery_cache")

DEFAULT_TTL_SECONDS = 600  # 10 minutos


def _key(query: str, region: str | None) -> str:
    payload = f"{(query or '').lower().strip()}|{(region or '').lower().strip()}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def get(query: str, region: str | None) -> list[dict] | None:
    init_db()
    now = now_iso()
    with connect() as conn:
        row = conn.execute(
            "SELECT result_json FROM discovery_cache "
            "WHERE query_hash = ? AND expires_at > ?",
            (_key(query, region), now),
        ).fetchone()
    if not row:
        log.info(kv(event="cache_miss", query=query, region=region))
        return None
    try:
        data = json.loads(row["result_json"])
        log.info(kv(event="cache_hit", query=query, region=region,
                    n=len(data) if isinstance(data, list) else 0))
        return data
    except (json.JSONDecodeError, TypeError):
        return None


def put(
    query: str,
    region: str | None,
    results: list[dict],
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
) -> None:
    init_db()
    now = datetime.now(timezone.utc)
    expires = (now + timedelta(seconds=ttl_seconds)).isoformat(timespec="seconds")
    with connect() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO discovery_cache
              (query_hash, query_text, region, result_json, expires_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                _key(query, region),
                query,
                region,
                json.dumps(results, ensure_ascii=False),
                expires,
                now.isoformat(timespec="seconds"),
            ),
        )
    log.info(kv(event="cache_put", query=query, region=region,
                n=len(results), ttl=ttl_seconds))


def cleanup_expired() -> int:
    init_db()
    with connect() as conn:
        cur = conn.execute(
            "DELETE FROM discovery_cache WHERE expires_at < ?", (now_iso(),)
        )
        removed = cur.rowcount
    log.info(kv(event="cache_cleanup", removed=removed))
    return removed


def invalidate(query: str | None = None, region: str | None = None) -> int:
    init_db()
    with connect() as conn:
        if query is None and region is None:
            cur = conn.execute("DELETE FROM discovery_cache")
        else:
            cur = conn.execute(
                "DELETE FROM discovery_cache WHERE query_hash = ?",
                (_key(query or "", region),),
            )
        return cur.rowcount


def info() -> dict:
    init_db()
    now = now_iso()
    with connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM discovery_cache").fetchone()[0]
        live = conn.execute(
            "SELECT COUNT(*) FROM discovery_cache WHERE expires_at > ?", (now,)
        ).fetchone()[0]
    return {"total": total, "live": live, "expired": total - live}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("info")
    sub.add_parser("cleanup")
    sub.add_parser("clear")
    args = ap.parse_args()

    if args.cmd == "info":
        for k, v in info().items():
            print(f"  {k:10s} {v}")
        return 0
    if args.cmd == "cleanup":
        n = cleanup_expired()
        print(f"removed {n} expired entries")
        return 0
    if args.cmd == "clear":
        n = invalidate()
        print(f"removed {n} entries")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
