"""
Watcher engine: monitoramento de combinações (keyword, region, price_range)
no Facebook Marketplace.

Modelo mental:
    1. create_watch()    → cria a combinação de busca em `watchers`
    2. run_backfill()    → descoberta inicial, povoa `watcher_results` com
                           is_initial_backfill=1. NENHUM alerta é emitido aqui.
    3. monitor_watch()   → rodadas subsequentes; qualquer listing_id que
                           não esteja em `watcher_results` para este watch_id
                           é considerado MATCH NOVO → gera evento
                           `watcher_match` e fica candidato a alerta
                           (processado depois por alert_engine).

---

⚠ REALIDADE SOBRE "ALERTA IMEDIATO"

O produto ideal seria "alerta em segundos quando um anúncio novo aparecer".
Esta arquitetura NÃO consegue isso, e não é honesto prometer. Limitações:

  - discovery usa DuckDuckGo via `discover_links.DuckDuckGoBackend`. DDG
    reindex de conteúdo novo do FB leva de minutos a horas
  - rate limit conservador (5-9s entre páginas de resultados) para não
    cair em CAPTCHA
  - extract_item.extract() é bloqueante e precisa ser espaçado para não
    cair em login wall do FB

Consequência prática: latência mínima entre um anúncio ser publicado e o
watcher detectá-lo é dezenas de minutos, não segundos. A API oficial do
Meta Commerce resolve isso, mas requer parceria formal.

O que este engine entrega de verdade:
  - backfill confiável do que está indexado pelo DDG quando você criou
    o watcher
  - monitoramento periódico com dedup determinístico (mesma watch_id +
    listing_id nunca vira match duas vezes)
  - alerta disparado na próxima passada do pipeline após a detecção

---

Uso via CLI:
    python watcher_engine.py create --keyword iphone --region Araçatuba
    python watcher_engine.py backfill 1
    python watcher_engine.py monitor 1
    python watcher_engine.py list
    python watcher_engine.py run-due                    # roda todos os watchers ativos
"""

from __future__ import annotations

import argparse
import asyncio
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterator

from db import connect, init_db, insert_event, now_iso
from discover_links import DEFAULT_UA, DuckDuckGoBackend, Hit, build_query
from extract_item import Listing, extract
from logging_setup import get_logger, kv
from price_normalizer import parse as parse_price

log = get_logger("watcher_engine")

# Conservador para evitar login wall entre extracts
EXTRACT_DELAY_SECONDS = 4.0
DEFAULT_MAX_PAGES_BACKFILL = 3
DEFAULT_MAX_PAGES_MONITOR = 2
DEFAULT_INTERVAL_SECONDS = 3600  # 1h entre rodadas do mesmo watcher

# v9: intervalo por prioridade. priority menor = mais frequente.
# 1 = high (10min), 2 = medium (30min, default), 3 = low (1h)
PRIORITY_INTERVALS = {
    1: 600,    # 10 min
    2: 1800,   # 30 min
    3: 3600,   # 1 h
}


def interval_for_priority(priority: int | None,
                          fallback: int = DEFAULT_INTERVAL_SECONDS) -> int:
    if priority is None:
        return fallback
    return PRIORITY_INTERVALS.get(priority, fallback)


def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


def _norm(s: str | None) -> str:
    if not s:
        return ""
    return _strip_accents(s).lower()


# --- filtro de match (pura, sem DB) ---------------------------------------

def matches_watcher(listing: Listing, watcher: dict) -> tuple[bool, str]:
    """Puro: decide se um `Listing` já extraído bate num watcher.

    Retorna (matched, reason_if_skip). Keyword e region usam substring
    case/accent-insensitive. Filtros de preço são inclusivos.
    """
    title_norm = _norm(listing.title)
    kw_norm = _norm(watcher.get("keyword"))
    if not kw_norm:
        return False, "watcher_keyword_empty"
    if kw_norm not in title_norm:
        return False, "keyword_mismatch"

    region = watcher.get("region")
    if region:
        region_norm = _norm(region)
        loc_norm = _norm(listing.location_text)
        if region_norm not in loc_norm:
            return False, "region_mismatch"

    min_price = watcher.get("min_price")
    max_price = watcher.get("max_price")
    if min_price is not None or max_price is not None:
        price = parse_price(listing.price_amount or listing.price_formatted)
        if price is None:
            return False, "no_price_for_filter"
        if min_price is not None and price < min_price:
            return False, "below_min"
        if max_price is not None and price > max_price:
            return False, "above_max"

    return True, "ok"


# --- discovery + extract orchestration -------------------------------------

def _discover_hits(watcher: dict, max_pages: int) -> list[Hit]:
    """Discovery via marketplace_discovery_engine (multi-estratégia + cache TTL).
    Converte os dicts retornados em objetos Hit para manter compatibilidade
    com o código existente."""
    from marketplace_discovery_engine import discover_for
    result = discover_for(
        keyword=watcher["keyword"],
        region=watcher.get("region"),
        max_pages=max_pages,
    )
    return [
        Hit(url=h["url"], item_id=h["item_id"],
            title=h.get("title"), backend="discovery_engine")
        for h in result.get("hits", [])
    ]


def _iter_extract(hits: Iterator[Hit], seen_ids: set[str]) -> Iterator[Listing]:
    """Extrai cada hit que não está em seen_ids, com rate limit entre requests."""
    first = True
    for hit in hits:
        if hit.item_id in seen_ids:
            continue
        if not first:
            time.sleep(EXTRACT_DELAY_SECONDS)
        first = False
        yield extract(hit.url)


# --- create_watch ---------------------------------------------------------

def create_watch(
    keyword: str,
    region: str | None = None,
    user_id: str | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    priority: int = 2,
    plan: str | None = None,
) -> int:
    if not keyword or not keyword.strip():
        raise ValueError("keyword é obrigatório")
    if priority not in PRIORITY_INTERVALS:
        raise ValueError(
            f"priority inválida ({priority}); use {sorted(PRIORITY_INTERVALS)}"
        )
    if plan is not None and plan not in ("free", "pro", "premium"):
        raise ValueError(f"plan inválido ({plan}); use free|pro|premium ou None")
    init_db()
    with connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO watchers
              (user_id, keyword, region, min_price, max_price,
               is_active, priority, plan, created_at)
            VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?)
            """,
            (user_id, keyword.strip(), region, min_price, max_price,
             priority, plan, now_iso()),
        )
        wid = cur.lastrowid
    log.info(kv(event="watch_created", watch_id=wid,
                keyword=keyword, region=region, priority=priority, plan=plan))
    return wid


def _load_watcher(conn, watch_id: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM watchers WHERE watch_id = ?", (watch_id,)
    ).fetchone()
    return dict(row) if row else None


def _seen_listing_ids(conn, watch_id: int) -> set[str]:
    return {
        r["listing_id"] for r in conn.execute(
            "SELECT listing_id FROM watcher_results WHERE watch_id = ?",
            (watch_id,),
        ).fetchall()
    }


def _insert_result(
    conn, watch_id: int, listing_id: str, is_backfill: bool,
) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO watcher_results
          (watch_id, listing_id, first_seen, is_initial_backfill)
        VALUES (?, ?, ?, ?)
        """,
        (watch_id, listing_id, now_iso(), 1 if is_backfill else 0),
    )


def _persist_listing(conn, listing: Listing) -> None:
    """Insere ou atualiza o listing na tabela `listings` a partir dos dados
    extraídos. Sem isso, watcher_results tem o ID mas o JOIN com listings
    retorna vazio e o dashboard não mostra nada."""
    now = now_iso()
    existing = conn.execute(
        "SELECT id FROM listings WHERE id = ?", (listing.id,)
    ).fetchone()
    if existing is None:
        conn.execute(
            """
            INSERT INTO listings
              (id, url, source, first_seen_at, last_seen_at, last_status,
               current_title, current_price, current_currency, current_location,
               current_seller)
            VALUES (?, ?, 'watcher', ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (listing.id, listing.url, now, now, listing.status,
             listing.title, listing.price_amount or listing.price_formatted,
             listing.price_currency, listing.location_text,
             listing.seller_name),
        )
    else:
        conn.execute(
            """
            UPDATE listings
               SET last_seen_at = ?,
                   last_status = ?,
                   current_title = COALESCE(?, current_title),
                   current_price = COALESCE(?, current_price),
                   current_currency = COALESCE(?, current_currency),
                   current_location = COALESCE(?, current_location)
             WHERE id = ?
            """,
            (now, listing.status, listing.title,
             listing.price_amount or listing.price_formatted,
             listing.price_currency, listing.location_text, listing.id),
        )


def _touch_watcher(conn, watch_id: int) -> None:
    conn.execute(
        "UPDATE watchers SET last_run_at = ? WHERE watch_id = ?",
        (now_iso(), watch_id),
    )


# --- run_backfill ---------------------------------------------------------

def run_backfill(watch_id: int, max_pages: int = DEFAULT_MAX_PAGES_BACKFILL,
                 validate_limit: int = 15) -> dict:
    """Descoberta inicial COM validação no Facebook.

    1. Busca URLs no DuckDuckGo (rápido)
    2. Para cada URL (até validate_limit), extrai dados reais do Facebook
    3. Filtra: só insere anúncios ATIVOS (status=ok, com título)
    4. Descarta vendidos/removidos/login_wall

    Grava tudo como is_initial_backfill=1 — sem alertas."""
    init_db()
    with connect() as conn:
        watcher = _load_watcher(conn, watch_id)
        if watcher is None:
            raise ValueError(f"watch_id {watch_id} não existe")
        seen = _seen_listing_ids(conn, watch_id)

    log.info(kv(event="backfill_start", watch_id=watch_id,
                keyword=watcher["keyword"], region=watcher["region"]))

    hits = _discover_hits(watcher, max_pages=max_pages)
    # Filtrar apenas os que são novos (não vistos antes)
    new_hits = [h for h in hits if h.item_id not in seen][:validate_limit]

    stats = {"discovered": len(hits), "validated": 0, "active": 0,
             "rejected": 0, "matched": 0}

    for listing in _iter_extract(iter(new_hits), seen):
        stats["validated"] += 1
        if listing.status != "ok" or not listing.title:
            stats["rejected"] += 1
            log.info(kv(watch_id=watch_id, listing=listing.id,
                        event="backfill_rejected", status=listing.status))
            continue

        stats["active"] += 1
        with connect() as conn:
            _persist_listing(conn, listing)
            _insert_result(conn, watch_id, listing.id, is_backfill=True)
        stats["matched"] += 1
        seen.add(listing.id)
        log.info(kv(watch_id=watch_id, listing=listing.id,
                     event="backfill_match", title=(listing.title or "")[:40]))

    with connect() as conn:
        _touch_watcher(conn, watch_id)

    log.info(kv(event="backfill_done", watch_id=watch_id, **stats))
    return stats


# --- monitor_watch --------------------------------------------------------

def monitor_watch(watch_id: int, max_pages: int = DEFAULT_MAX_PAGES_MONITOR) -> dict:
    """Uma passada de discovery. Novos matches geram evento `watcher_match`."""
    init_db()
    with connect() as conn:
        watcher = _load_watcher(conn, watch_id)
        if watcher is None:
            return {"status": "missing", "watch_id": watch_id}
        if not watcher["is_active"]:
            return {"status": "inactive", "watch_id": watch_id}
        seen = _seen_listing_ids(conn, watch_id)

    hits = _discover_hits(watcher, max_pages=max_pages)
    stats = {"discovered": len(hits), "new_matches": 0, "skipped": 0,
             "match_ids": [], "skip_reasons": {}}

    for listing in _iter_extract(iter(hits), seen):
        if listing.status != "ok":
            stats["skipped"] += 1
            stats["skip_reasons"][f"extract_{listing.status}"] = \
                stats["skip_reasons"].get(f"extract_{listing.status}", 0) + 1
            continue
        ok, reason = matches_watcher(listing, watcher)
        if not ok:
            stats["skipped"] += 1
            stats["skip_reasons"][reason] = stats["skip_reasons"].get(reason, 0) + 1
            continue

        with connect() as conn:
            _persist_listing(conn, listing)
            _insert_result(conn, watch_id, listing.id, is_backfill=False)
            insert_event(
                conn, listing.id, now_iso(), "watcher_match",
                None, f"watch_id={watch_id}",
            )
        stats["new_matches"] += 1
        stats["match_ids"].append(listing.id)
        log.info(kv(watch_id=watch_id, listing=listing.id, event="watcher_match"))

    with connect() as conn:
        _touch_watcher(conn, watch_id)

    log.info(kv(event="monitor_done", watch_id=watch_id,
                new_matches=stats["new_matches"]))
    return stats


# --- run_due_watchers (orquestrador de passada) ---------------------------

def _parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _select_due_ids(rows, fallback_interval: int) -> list[int]:
    """Seleciona watch_ids onde last_run é None ou mais velho que o intervalo
    da sua prioridade. Pure-ish (recebe rows). Reusado pelas versões sync e async."""
    now = datetime.now(timezone.utc)
    due_ids: list[int] = []
    for r in rows:
        priority = r["priority"] if "priority" in r.keys() else 2
        interval = interval_for_priority(priority, fallback_interval)
        threshold = now - timedelta(seconds=interval)
        last = _parse_iso(r["last_run_at"])
        if last is None or last < threshold:
            due_ids.append(r["watch_id"])
    return due_ids


def _load_active_watchers() -> list:
    init_db()
    with connect() as conn:
        return conn.execute(
            "SELECT watch_id, last_run_at, priority FROM watchers WHERE is_active = 1"
        ).fetchall()


def run_due_watchers(
    min_interval_seconds: int = DEFAULT_INTERVAL_SECONDS,
) -> dict:
    """Versão sync: roda watchers devido sequencialmente. Mantida para CLI
    e para compatibilidade com setups que não querem asyncio."""
    rows = _load_active_watchers()
    due_ids = _select_due_ids(rows, min_interval_seconds)

    total_matches = 0
    ran = 0
    failures = 0
    for wid in due_ids:
        try:
            result = monitor_watch(wid)
            if isinstance(result, dict):
                total_matches += result.get("new_matches", 0)
            ran += 1
        except Exception as e:  # noqa: BLE001
            log.error(kv(event="monitor_watch_failed",
                         watch_id=wid, error=repr(e)[:200]))
            failures += 1

    return {
        "total_active": len(rows),
        "due": len(due_ids),
        "ran": ran,
        "failures": failures,
        "total_new_matches": total_matches,
    }


async def run_due_watchers_async(
    min_interval_seconds: int = DEFAULT_INTERVAL_SECONDS,
    concurrency: int = 3,
    use_scheduler: bool = True,
) -> dict:
    """Versão async paralela: usa asyncio.Semaphore + asyncio.to_thread.

    Wrap dos `monitor_watch` síncronos em threads, com semáforo limitando a
    concorrência. Cada watcher roda isolado em try/except — falha numa não
    derruba as outras (asyncio.gather com return_exceptions=True).

    v10: quando use_scheduler=True (default), watcher_scheduler ordena os
    DUE por dynamic_priority — usuários premium e watchers populares rodam
    primeiro. Caso contrário, ordem natural por watch_id.

    Por que `to_thread` e não `extract_async`: o monitor_watch ainda usa o
    extractor sync por simplicidade. Threads liberam o GIL durante IO de
    rede, então a concorrência real funciona.
    """
    rows = _load_active_watchers()
    if use_scheduler:
        try:
            from watcher_scheduler import schedule_due
            due_ids = schedule_due(min_interval_seconds)
        except Exception as e:  # noqa: BLE001
            log.warning(kv(event="scheduler_failed_fallback",
                           error=type(e).__name__))
            due_ids = _select_due_ids(rows, min_interval_seconds)
    else:
        due_ids = _select_due_ids(rows, min_interval_seconds)

    if not due_ids:
        return {
            "total_active": len(rows), "due": 0, "ran": 0,
            "failures": 0, "total_new_matches": 0, "concurrency": concurrency,
        }

    sem = asyncio.Semaphore(concurrency)
    log.info(kv(event="parallel_watchers_start",
                due=len(due_ids), concurrency=concurrency))

    async def _run_one(wid: int):
        async with sem:
            try:
                return await asyncio.to_thread(monitor_watch, wid)
            except Exception as e:  # noqa: BLE001
                log.error(kv(event="monitor_watch_failed",
                             watch_id=wid, error=repr(e)[:200]))
                return e  # exceção sentinela, contada como failure

    results = await asyncio.gather(*[_run_one(wid) for wid in due_ids])

    total_matches = 0
    ran = 0
    failures = 0
    for result in results:
        if isinstance(result, Exception):
            failures += 1
            continue
        ran += 1
        if isinstance(result, dict):
            total_matches += result.get("new_matches", 0)

    return {
        "total_active": len(rows),
        "due": len(due_ids),
        "ran": ran,
        "failures": failures,
        "total_new_matches": total_matches,
        "concurrency": concurrency,
    }


# --- CLI ------------------------------------------------------------------

def _print_watchers() -> None:
    init_db()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT w.watch_id, w.keyword, w.region, w.min_price, w.max_price,
                   w.is_active, w.last_run_at, w.created_at,
                   (SELECT COUNT(*) FROM watcher_results wr
                     WHERE wr.watch_id = w.watch_id) AS match_count,
                   (SELECT COUNT(*) FROM watcher_results wr
                     WHERE wr.watch_id = w.watch_id AND is_initial_backfill = 0) AS new_count
              FROM watchers w
             ORDER BY w.watch_id
            """
        ).fetchall()
    if not rows:
        print("(no watchers)")
        return
    print(f"{'id':>4s}  {'kw':<20s} {'region':<20s} {'active':>6s} "
          f"{'matches':>8s} {'new':>5s}  {'last_run':<20s}")
    print("-" * 95)
    for r in rows:
        print(
            f"{r['watch_id']:>4d}  "
            f"{(r['keyword'] or '')[:20]:<20s} "
            f"{(r['region'] or '-')[:20]:<20s} "
            f"{'✓' if r['is_active'] else '✗':>6s} "
            f"{r['match_count']:>8d} "
            f"{r['new_count']:>5d}  "
            f"{(r['last_run_at'] or 'never')[:19]:<20s}"
        )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    sp_c = sub.add_parser("create")
    sp_c.add_argument("--keyword", required=True)
    sp_c.add_argument("--region")
    sp_c.add_argument("--user-id")
    sp_c.add_argument("--min-price", type=float)
    sp_c.add_argument("--max-price", type=float)

    sp_b = sub.add_parser("backfill")
    sp_b.add_argument("watch_id", type=int)
    sp_b.add_argument("--pages", type=int, default=DEFAULT_MAX_PAGES_BACKFILL)

    sp_m = sub.add_parser("monitor")
    sp_m.add_argument("watch_id", type=int)
    sp_m.add_argument("--pages", type=int, default=DEFAULT_MAX_PAGES_MONITOR)

    sub.add_parser("list")

    sp_r = sub.add_parser("run-due")
    sp_r.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_SECONDS)

    args = ap.parse_args()

    if args.cmd == "create":
        wid = create_watch(
            keyword=args.keyword, region=args.region, user_id=args.user_id,
            min_price=args.min_price, max_price=args.max_price,
        )
        print(f"created watch_id={wid}")
        return 0

    if args.cmd == "backfill":
        result = run_backfill(args.watch_id, max_pages=args.pages)
        for k, v in result.items():
            print(f"  {k:20s} {v}")
        return 0

    if args.cmd == "monitor":
        result = monitor_watch(args.watch_id, max_pages=args.pages)
        for k, v in result.items():
            print(f"  {k:20s} {v}")
        return 0

    if args.cmd == "list":
        _print_watchers()
        return 0

    if args.cmd == "run-due":
        result = run_due_watchers(min_interval_seconds=args.interval)
        for k, v in result.items():
            print(f"  {k:25s} {v}")
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
