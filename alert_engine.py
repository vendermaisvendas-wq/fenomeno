"""
Alert engine específico para watchers.

Diferença do `alerts.py` v4: `alerts.py` dispara pelo score genérico
(opportunity_score, discount_percentage). Este módulo dispara por
*match de watcher* — sempre que um listing bate um watcher ativo, ele
vai pro fila de alertas, independente de score.

Reutiliza os transportes de `alerts.py` (send_telegram, send_discord).

Dedup: evento `alert_sent` com `old_value = "watcher_<channel>_<watch_id>"`
para evitar reenvio do mesmo listing/watcher/canal.

Fluxo esperado no pipeline:
    monitor → watcher_engine.run_due_watchers → insere eventos `watcher_match`
          → alert_engine.process_pending_watcher_matches → envia webhooks

Uso:
    python alert_engine.py process          # processa eventos pendentes
    python alert_engine.py process --dry-run
"""

from __future__ import annotations

import argparse

from alerts import send_discord, send_telegram
from db import connect, init_db, insert_event, now_iso
from logging_setup import get_logger, kv

log = get_logger("alert_engine")

# Janela: quantos eventos watcher_match olhar por passada
BATCH_SIZE = 200


def format_watcher_alert(listing_row, watcher_row) -> str:
    title = listing_row["current_title"] or "(sem título)"
    price = listing_row["current_price"] or "?"
    currency = listing_row["current_currency"] or ""
    location = listing_row["current_location"] or "?"
    url = listing_row["url"]

    kw = watcher_row["keyword"]
    region = watcher_row["region"] or "qualquer região"

    lines = [
        f"🎯 Match no watcher #{watcher_row['watch_id']}",
        f"filtro: \"{kw}\" em {region}",
        "",
        title,
        f"preço: {price} {currency}".strip(),
        f"local: {location}",
        url,
    ]
    return "\n".join(lines)


def _channel_dedup_key(channel: str, watch_id: int) -> str:
    return f"watcher_{channel}_{watch_id}"


def _already_alerted(conn, listing_id: str, dedup_key: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM events WHERE listing_id = ? "
        "AND event_type = 'alert_sent' AND old_value = ? LIMIT 1",
        (listing_id, dedup_key),
    ).fetchone()
    return row is not None


def send_for_match(listing_id: str, watch_id: int, dry_run: bool = False) -> dict:
    """Envia alerta para um match específico em todos os canais configurados.
    Retorna {'telegram': bool|None, 'discord': bool|None} onde None = não configurado."""
    init_db()
    with connect() as conn:
        watcher = conn.execute(
            "SELECT * FROM watchers WHERE watch_id = ?", (watch_id,)
        ).fetchone()
        listing = conn.execute(
            "SELECT * FROM listings WHERE id = ?", (listing_id,)
        ).fetchone()

    if not watcher or not listing:
        log.warning(kv(event="match_missing",
                       listing=listing_id, watch_id=watch_id))
        return {"telegram": None, "discord": None, "error": "missing_row"}

    msg = format_watcher_alert(listing, watcher)
    result: dict = {"telegram": None, "discord": None}

    for channel, fn in (("telegram", send_telegram), ("discord", send_discord)):
        dedup_key = _channel_dedup_key(channel, watch_id)
        with connect() as conn:
            if _already_alerted(conn, listing_id, dedup_key):
                result[channel] = "dedup"
                continue
        if dry_run:
            log.info(kv(event="alert_dry_run",
                        listing=listing_id, watch_id=watch_id, channel=channel))
            result[channel] = "dry_run"
            continue

        sent = fn(msg)
        result[channel] = sent
        if sent:
            with connect() as conn:
                insert_event(
                    conn, listing_id, now_iso(),
                    "alert_sent", dedup_key, "ok",
                )
            log.info(kv(event="watcher_alert_sent",
                        listing=listing_id, watch_id=watch_id, channel=channel))
        elif sent is False:
            log.warning(kv(event="watcher_alert_failed",
                           listing=listing_id, watch_id=watch_id, channel=channel))
    return result


def process_pending_watcher_matches(dry_run: bool = False) -> dict:
    """Para cada evento `watcher_match` recente, tenta enviar alerta.
    A dedup via `alert_sent` garante que reprocessar é idempotente."""
    init_db()
    stats = {
        "matches_scanned": 0,
        "telegram_sent": 0,
        "discord_sent": 0,
        "dedup_skipped": 0,
        "unconfigured": 0,
        "errors": 0,
    }
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT listing_id, new_value
              FROM events
             WHERE event_type = 'watcher_match'
             ORDER BY at DESC
             LIMIT ?
            """,
            (BATCH_SIZE,),
        ).fetchall()

    for r in rows:
        stats["matches_scanned"] += 1
        nv = (r["new_value"] or "").strip()
        if not nv.startswith("watch_id="):
            continue
        try:
            watch_id = int(nv.split("=", 1)[1])
        except ValueError:
            stats["errors"] += 1
            continue

        result = send_for_match(r["listing_id"], watch_id, dry_run=dry_run)
        for channel, outcome in result.items():
            if channel == "error":
                continue
            if outcome is True:
                stats[f"{channel}_sent"] += 1
            elif outcome == "dedup":
                stats["dedup_skipped"] += 1
            elif outcome is None:
                stats["unconfigured"] += 1
            elif outcome is False:
                stats["errors"] += 1

    log.info(kv(event="watcher_alerts_processed", **stats))
    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("process")
    sp.add_argument("--dry-run", action="store_true")

    args = ap.parse_args()

    if args.cmd == "process":
        stats = process_pending_watcher_matches(dry_run=args.dry_run)
        for k, v in stats.items():
            print(f"  {k:25s} {v}")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
