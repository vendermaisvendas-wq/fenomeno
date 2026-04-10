"""
Monitor assíncrono de anúncios do Marketplace.

Fluxo:
  1. Lê IDs a monitorar de (a) seed file e/ou (b) tabela listings no DB
  2. Re-fetcha cada um via extract_async (httpx)
  3. Detecta eventos de mudança (preço, título, removido, reaparecido)
  4. Grava snapshot + eventos em SQLite
  5. Rate limit: semáforo com N concorrências + delay aleatório por request

Estratégia de "removed":
  - Se o fetch devolve `not_found` ou status de erro persistente, marca
    is_removed=1 e grava evento `removed` (se for a primeira detecção).
  - Se um listing removido retorna `ok` num fetch posterior, limpa a flag
    e grava evento `reappeared`.

Uso:
    python monitor.py --seed seed_urls.txt --once
    python monitor.py --from-db --interval 21600   # usa listings já no DB, passada a cada 6h
    python monitor.py --seed seed_urls.txt --concurrency 3 --min-delay 3 --max-delay 10
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import random
import signal
import sys
from dataclasses import asdict
from pathlib import Path

import httpx

from analytics import _to_float
from db import (
    connect, init_db, insert_event, insert_price_history, insert_snapshot, now_iso,
)
from extract_item import HEADERS, Listing, extract_async, normalize_target
from logging_setup import configure as configure_logging, get_logger, kv

log = get_logger("monitor")


# --- helpers ----------------------------------------------------------------

def payload_hash(listing: Listing) -> str:
    keyed = {
        "title": listing.title,
        "price": listing.price_amount or listing.price_formatted,
        "currency": listing.price_currency,
        "description": listing.description,
        "status": listing.status,
    }
    blob = json.dumps(keyed, sort_keys=True, ensure_ascii=False).encode()
    return hashlib.sha256(blob).hexdigest()[:16]


def load_seed_file(path: Path) -> list[tuple[str, str]]:
    """Lê URLs/IDs do arquivo seed. Retorna pares (item_id, url)."""
    out: list[tuple[str, str]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            item_id, url = normalize_target(line)
        except ValueError:
            print(f"[seed] ignorando linha inválida: {line!r}", file=sys.stderr)
            continue
        out.append((item_id, url))
    return out


def load_from_db() -> list[tuple[str, str]]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT id, url FROM listings ORDER BY last_seen_at ASC"
        ).fetchall()
    return [(r["id"], r["url"]) for r in rows]


def ensure_seeded(targets: list[tuple[str, str]], source: str) -> None:
    """Garante que cada (id, url) existe na tabela listings."""
    with connect() as conn:
        for item_id, url in targets:
            row = conn.execute("SELECT id FROM listings WHERE id = ?", (item_id,)).fetchone()
            if row is None:
                now = now_iso()
                conn.execute(
                    """
                    INSERT INTO listings (id, url, source, first_seen_at, last_seen_at, last_status)
                    VALUES (?, ?, ?, ?, ?, 'pending')
                    """,
                    (item_id, url, source, now, now),
                )


# --- evento / reconciliação -------------------------------------------------

def _record_price_history(conn, listing: Listing) -> None:
    """Grava uma linha em price_history se houver preço parseável.
    Não deduplica — o caller já decide quando chamar (em change events)."""
    raw = listing.price_amount or listing.price_formatted
    if not raw:
        return
    price = _to_float(raw)
    if price is None:
        return
    insert_price_history(
        conn, listing.id, price, raw, listing.price_currency, listing.fetched_at
    )


def reconcile(conn, listing: Listing) -> list[str]:
    """Compara o Listing novo com o estado atual em `listings` e grava
    eventos + snapshot + price_history. Retorna descrições curtas de eventos."""
    now = listing.fetched_at
    events: list[str] = []

    row = conn.execute("SELECT * FROM listings WHERE id = ?", (listing.id,)).fetchone()

    if row is None:
        conn.execute(
            """
            INSERT INTO listings (id, url, source, first_seen_at, last_seen_at, last_status,
                                  current_title, current_price, current_currency, current_location,
                                  current_seller)
            VALUES (?, ?, 'monitor', ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (listing.id, listing.url, now, now, listing.status,
             listing.title, listing.price_amount, listing.price_currency,
             listing.location_text, listing.seller_name),
        )
        insert_event(conn, listing.id, now, "first_seen", None, listing.title)
        events.append("first_seen")
        if listing.status == "ok":
            _record_price_history(conn, listing)
    else:
        was_removed = bool(row["is_removed"])
        new_is_error = listing.status in ("not_found", "error", "empty")
        new_is_ok = listing.status == "ok"

        # price change
        new_price = listing.price_amount or listing.price_formatted
        old_price = row["current_price"]
        if new_is_ok and new_price and new_price != old_price:
            insert_event(conn, listing.id, now, "price_change", old_price, new_price)
            events.append(f"price {old_price} -> {new_price}")
            _record_price_history(conn, listing)

        # title change
        if new_is_ok and listing.title and listing.title != row["current_title"]:
            insert_event(conn, listing.id, now, "title_change", row["current_title"], listing.title)
            events.append("title_change")

        # removed / reappeared
        if not was_removed and listing.status == "not_found":
            conn.execute(
                "UPDATE listings SET is_removed = 1, removed_at = ? WHERE id = ?",
                (now, listing.id),
            )
            insert_event(conn, listing.id, now, "removed", row["last_status"], listing.status)
            events.append("removed")
        elif was_removed and new_is_ok:
            conn.execute(
                "UPDATE listings SET is_removed = 0, reappeared_at = ? WHERE id = ?",
                (now, listing.id),
            )
            insert_event(conn, listing.id, now, "reappeared", "not_found", "ok")
            events.append("reappeared")

        # status genérico (inclui transição para/de login_wall)
        if row["last_status"] != listing.status and "removed" not in events and "reappeared" not in events:
            insert_event(conn, listing.id, now, "status_change", row["last_status"], listing.status)
            events.append(f"status {row['last_status']} -> {listing.status}")

        conn.execute(
            """
            UPDATE listings
               SET last_seen_at = ?, last_status = ?,
                   current_title = COALESCE(?, current_title),
                   current_price = COALESCE(?, current_price),
                   current_currency = COALESCE(?, current_currency),
                   current_location = COALESCE(?, current_location),
                   current_seller = COALESCE(?, current_seller)
             WHERE id = ?
            """,
            (now, listing.status, listing.title,
             new_price, listing.price_currency, listing.location_text,
             listing.seller_name, listing.id),
        )

    insert_snapshot(conn, listing.id, now, listing.status,
                    payload_hash(listing), asdict(listing))
    return events


# --- loop assíncrono --------------------------------------------------------

class MonitorState:
    def __init__(self) -> None:
        self.stop = False
        self.login_wall_pause_until: float = 0.0


STATE = MonitorState()


def _install_signal_handlers() -> None:
    def handler(signum, frame):  # noqa: ARG001
        STATE.stop = True
        print("\n[monitor] shutdown requested — terminando após o ciclo atual", flush=True)
    try:
        signal.signal(signal.SIGINT, handler)
    except ValueError:
        pass  # não funciona em thread secundária


async def fetch_one(
    client: httpx.AsyncClient,
    item_id: str,
    url: str,
    sem: asyncio.Semaphore,
    min_delay: float,
    max_delay: float,
) -> Listing:
    async with sem:
        loop = asyncio.get_running_loop()
        # Respeita pausa global se login wall foi detectado
        now = loop.time()
        if now < STATE.login_wall_pause_until:
            await asyncio.sleep(STATE.login_wall_pause_until - now)

        listing = await extract_async(url, client)

        if listing.status == "login_wall":
            pause_for = 600.0  # 10min
            STATE.login_wall_pause_until = loop.time() + pause_for
            log.warning(kv(event="login_wall_detected", pause_for=pause_for))
            print("  [monitor] login wall detected — pausando 10min globalmente",
                  flush=True)

        await asyncio.sleep(random.uniform(min_delay, max_delay))
        return listing


async def run_pass(
    targets: list[tuple[str, str]],
    concurrency: int,
    min_delay: float,
    max_delay: float,
    scan_opportunities_after: bool = True,
) -> None:
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    sem = asyncio.Semaphore(concurrency)
    log.info(kv(event="pass_start", targets=len(targets), concurrency=concurrency))
    async with httpx.AsyncClient(headers=HEADERS, limits=limits, timeout=20) as client:
        tasks = [
            asyncio.create_task(fetch_one(client, item_id, url, sem, min_delay, max_delay))
            for item_id, url in targets
        ]
        for coro in asyncio.as_completed(tasks):
            if STATE.stop:
                break
            listing = await coro
            with connect() as conn:
                events = reconcile(conn, listing)
            summary = ", ".join(events) if events else "no-change"
            price = listing.price_formatted or listing.price_amount or "-"
            for evt in events:
                log.info(kv(listing=listing.id, event=evt))
            print(
                f"  [{listing.status:10s}] {listing.id}  price={price:20s} "
                f"method={listing.extraction_method:30s} events=[{summary}]",
                flush=True,
            )

    if scan_opportunities_after and not STATE.stop:
        _run_intelligence_pipeline()
        # v9: watchers em paralelo (async). Roda fora do pipeline sync porque
        # estamos num contexto async aqui — pode awaitar diretamente.
        try:
            from watcher_engine import run_due_watchers_async
            wch_result = await run_due_watchers_async(concurrency=concurrency)
            log.info(kv(event="parallel_watchers_done", **wch_result))
            print(
                f"  [watchers] due={wch_result.get('due')} "
                f"ran={wch_result.get('ran')} "
                f"failures={wch_result.get('failures')} "
                f"new_matches={wch_result.get('total_new_matches')}",
                flush=True,
            )
        except Exception as e:
            log.error(kv(event="parallel_watchers_failed", error=repr(e)[:200]))


def _run_intelligence_pipeline() -> None:
    """Pipeline v5. Ordem importa:
      1. outlier_detector     → flagga preços absurdos
      2. market_value         → usa pool sem outliers para mediana mais limpa
      3. listing_cluster      → agrupamento solto (cluster_id)
      4. duplicate_detector   → agrupamento tight (duplicate_group_id)
      5. fraud_detector       → usa market_value + payload
      6. opportunities.scan   → heurísticas legadas (flags discretas)
      7. score_all_listings   → usa discount_percentage e pesos dinâmicos
      8. new_listing_detector → usa discount fresco
      9. alerts               → só depois de score+discount prontos
    Cada etapa é isolada em try/except: falha numa não derruba as outras."""
    def _safe(name, fn):
        try:
            result = fn()
            log.info(kv(event=f"{name}_done", result=str(result)[:200]))
            return result
        except Exception as e:
            log.error(kv(event=f"{name}_failed", error=repr(e)[:200]))
            return None

    from alert_engine import process_pending_watcher_matches
    from alert_priority_engine import process_with_priority
    from alerts import scan_and_alert
    from category_models import apply_classification
    from duplicate_detector import cluster_all as dup_cluster
    from fraud_detector import scan as fraud_scan
    from fresh_opportunity_detector import scan as fresh_scan
    from geo_coverage import run as geo_run
    from listing_cluster import cluster_all as listing_cluster_all
    from liquidity_model import score_all as liquidity_score
    from market_value import recompute_all as market_recompute
    from new_listing_detector import scan as new_scan
    from opportunities import reload_weights, scan as opp_scan, score_all_listings
    from opportunity_predictor import predict_all as opp_predict
    from outlier_detector import detect_outliers
    from price_model import train_and_predict as price_predict
    from recent_listing_detector import detect as recent_detect
    from seller_patterns import scan as seller_scan
    from vehicle_model import apply_vehicle_valuation
    from watcher_optimizer import prewarm_groups

    reload_weights()

    # --- v7: enriquecimento geográfico/categórico rodam primeiro ---
    # (geo e category são usados por etapas posteriores: listing.city e listing.category)
    geo = _safe("geo_coverage", geo_run)
    cat = _safe("category_models", apply_classification)

    out = _safe("outlier_detector", detect_outliers)
    mv = _safe("market_value", market_recompute)
    vm = _safe("vehicle_model", apply_vehicle_valuation)  # refina estimativa p/ vehicles
    lc = _safe("listing_cluster", listing_cluster_all)
    dup = _safe("duplicate_detector", dup_cluster)
    fr = _safe("fraud_detector", fraud_scan)
    opp = _safe("opportunities_scan", opp_scan)
    sc = _safe("score_all_listings", score_all_listings)
    sel = _safe("seller_patterns", seller_scan)
    pm = _safe("price_model", price_predict)
    lq = _safe("liquidity_model", liquidity_score)
    nw = _safe("new_listing_detector", new_scan)
    fresh = _safe("fresh_opportunity", fresh_scan)
    rec = _safe("recent_listing_detector", recent_detect)
    # v9: prewarm cache para grupos populares ANTES dos watchers
    pw = _safe("watcher_optimizer_prewarm", prewarm_groups)
    # v10: opportunity_probability calculada APÓS todos os outros sinais
    op = _safe("opportunity_predictor", opp_predict)
    # NOTA: a etapa de watchers em si é async/paralela e roda separado em
    # `run_pass()` (após esta função). Aqui só fazemos o prewarm.
    # v10: alert_priority substitui o process pending por uma versão ranqueada
    wal = _safe("watcher_alerts_priority", process_with_priority)
    al = _safe("alerts", scan_and_alert)

    print(
        f"  [pipeline] "
        f"geo_cities={geo and geo.get('cities')}  "
        f"cats={cat and cat.get('classified')}  "
        f"outliers={out and out.get('outliers')}  "
        f"market={mv and mv.get('updated')}  "
        f"vehicles={vm and vm.get('updated')}  "
        f"cluster={lc and lc.get('multi_member_clusters')}  "
        f"dup={dup and dup.get('multi_member_clusters')}  "
        f"fraud={fr and fr.get('high_risk')}  "
        f"opps={opp and opp.get('total_flagged')}  "
        f"scored={sc}  "
        f"sellers={sel and sel.get('sellers')}  "
        f"pm={pm and pm.get('updated')}  "
        f"liq={lq and lq.get('listings_scored')}  "
        f"new={nw and nw.get('flagged')}  "
        f"fresh={fresh and fresh.get('emitted')}  "
        f"recent={rec and rec.get('flagged')}  "
        f"prewarm={pw and pw.get('groups')}  "
        f"opp_pred={op and op.get('high_probability')}  "
        f"watcher_alerts={wal and (wal.get('sent_telegram', 0) + wal.get('sent_discord', 0))}  "
        f"alerts={al and (al.get('telegram', 0) + al.get('discord', 0))}",
        flush=True,
    )


async def run_loop(
    get_targets, interval: int, concurrency: int, min_delay: float, max_delay: float,
    once: bool,
) -> None:
    _install_signal_handlers()
    while not STATE.stop:
        targets = get_targets()
        print(f"[monitor] pass started {now_iso()} — {len(targets)} targets")
        await run_pass(targets, concurrency, min_delay, max_delay)
        if once or STATE.stop:
            break
        wait = interval + random.uniform(-interval * 0.05, interval * 0.05)
        print(f"[monitor] pass done — sleeping ~{wait:.0f}s")
        slept = 0.0
        while slept < wait and not STATE.stop:
            await asyncio.sleep(1)
            slept += 1


# --- CLI --------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--seed", type=Path, help="arquivo com URLs/IDs (uma por linha)")
    src.add_argument("--from-db", action="store_true",
                     help="usa os listings já cadastrados no banco")
    ap.add_argument("--interval", type=int, default=6 * 3600,
                    help="segundos entre passadas (default: 21600 = 6h)")
    ap.add_argument("--once", action="store_true", help="uma passada e sai")
    ap.add_argument("--concurrency", type=int, default=3,
                    help="requests simultâneos (default: 3)")
    ap.add_argument("--min-delay", type=float, default=3.0)
    ap.add_argument("--max-delay", type=float, default=10.0)
    args = ap.parse_args()

    configure_logging()
    init_db()

    if args.seed:
        if not args.seed.exists():
            print(f"seed file not found: {args.seed}", file=sys.stderr)
            return 2
        targets = load_seed_file(args.seed)
        ensure_seeded(targets, source="seed")
        get_targets = lambda: load_seed_file(args.seed)  # noqa: E731
    else:
        get_targets = load_from_db

    asyncio.run(run_loop(
        get_targets,
        interval=args.interval,
        concurrency=args.concurrency,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        once=args.once,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
