"""
Persistência do ComparablesIndex entre runs.

Motivação: construir o índice a partir do SQL é O(N) mas ainda paga overhead
de fetch + parse. Para runs repetidos sobre a mesma população, carregar um
pickle é ordens de magnitude mais rápido.

Invalidação: usamos um fingerprint do pool atual (count + hash determinístico
de ids + preços). Se bater com o fingerprint salvo, o cache é válido. Se
não, recarrega do SQL.

Layout:
    cache/comparables.pkl      pickle do (fingerprint, list[PricedItem])

API:
    load_or_build() -> (index, ComparablesIndex, source='cache'|'fresh')
    invalidate()
"""

from __future__ import annotations

import argparse
import hashlib
import pickle
from dataclasses import astuple
from pathlib import Path

from db import connect, init_db
from logging_setup import get_logger, kv
from market_value import ComparablesIndex, _load_priced_items

log = get_logger("comparables_cache")

CACHE_DIR = Path("cache")
CACHE_FILE = CACHE_DIR / "comparables.pkl"


def _fingerprint_items(items) -> str:
    """Hash determinístico da (id, price) de cada item, ordenado por id."""
    h = hashlib.sha256()
    for it in sorted(items, key=lambda x: x.id):
        h.update(it.id.encode("utf-8"))
        h.update(b":")
        h.update(f"{it.price:.2f}".encode("ascii"))
        h.update(b"\n")
    return h.hexdigest()[:16]


def _fingerprint_db() -> tuple[int, str]:
    """Lê (count, fingerprint) direto do banco — evita carregar tudo só para invalidar."""
    init_db()
    with connect() as conn:
        rows = conn.execute(
            "SELECT id, current_price FROM listings "
            "WHERE is_removed = 0 AND current_title IS NOT NULL "
            "  AND current_price IS NOT NULL "
            "ORDER BY id"
        ).fetchall()
    h = hashlib.sha256()
    count = 0
    for r in rows:
        h.update(r["id"].encode("utf-8"))
        h.update(b":")
        h.update((r["current_price"] or "").encode("utf-8"))
        h.update(b"\n")
        count += 1
    return count, h.hexdigest()[:16]


def save_cache(items) -> None:
    CACHE_DIR.mkdir(exist_ok=True)
    fp = _fingerprint_items(items)
    with CACHE_FILE.open("wb") as f:
        pickle.dump({"fingerprint": fp, "items": items}, f,
                    protocol=pickle.HIGHEST_PROTOCOL)
    log.info(kv(event="cache_saved", items=len(items), fp=fp))


def load_cache():
    """Retorna (items, fingerprint) ou (None, None) se inválido/inexistente."""
    if not CACHE_FILE.exists():
        return None, None
    try:
        with CACHE_FILE.open("rb") as f:
            data = pickle.load(f)
        return data.get("items"), data.get("fingerprint")
    except (pickle.UnpicklingError, EOFError, KeyError):
        return None, None


def load_or_build() -> tuple[ComparablesIndex, str]:
    """Carrega índice do cache se válido, senão rebuilda a partir do DB.
    Retorna (index, 'cache'|'fresh'|'db_empty')."""
    # Verificação barata de invalidação via fingerprint direto do DB
    _, db_fp = _fingerprint_db()

    cached_items, cached_fp = load_cache()
    if cached_items and cached_fp == db_fp:
        log.info(kv(event="cache_hit", fp=db_fp, items=len(cached_items)))
        return ComparablesIndex(cached_items), "cache"

    # Miss: carrega do DB, rebuilda, salva
    with connect() as conn:
        items = _load_priced_items(conn, exclude_outliers=True)
    if not items:
        return ComparablesIndex([]), "db_empty"
    save_cache(items)
    log.info(kv(event="cache_miss_rebuilt", items=len(items)))
    return ComparablesIndex(items), "fresh"


def invalidate() -> bool:
    """Remove o cache. Retorna True se havia algo para remover."""
    if CACHE_FILE.exists():
        CACHE_FILE.unlink()
        log.info(kv(event="cache_invalidated"))
        return True
    return False


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("build", help="força rebuild do cache")
    sub.add_parser("info", help="mostra estado do cache")
    sub.add_parser("invalidate", help="remove o cache")
    args = ap.parse_args()

    if args.cmd == "build":
        invalidate()
        _, src = load_or_build()
        print(f"built from: {src}")
        return 0

    if args.cmd == "info":
        count, db_fp = _fingerprint_db()
        cached_items, cached_fp = load_cache()
        print(f"db:    {count} items, fp={db_fp}")
        if cached_items is None:
            print("cache: (empty)")
        else:
            fresh = "✓" if cached_fp == db_fp else "✗ STALE"
            print(f"cache: {len(cached_items)} items, fp={cached_fp} {fresh}")
        return 0

    if args.cmd == "invalidate":
        print("removed" if invalidate() else "(nothing to remove)")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
