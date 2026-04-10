"""
Envio de alertas para Telegram e Discord via webhook.

Configuração por env vars (ambas opcionais — canais faltantes são puladas):
    TELEGRAM_BOT_TOKEN    token do bot (botfather)
    TELEGRAM_CHAT_ID      chat onde enviar
    DISCORD_WEBHOOK_URL   URL completa do webhook do canal

Quando disparar:
    opportunity_score >= SCORE_THRESHOLD (default 80)  OR
    discount_percentage >= DISCOUNT_THRESHOLD (default 30)

Idempotência: antes de enviar, checa se já existe evento `alert_sent` com
`old_value = '<channel>'` para aquele listing. Um mesmo listing é alertado
no máximo uma vez por canal, mesmo se rodarmos scan várias vezes.

Uso:
    python alerts.py             # envia alertas pendentes
    python alerts.py --dry-run   # lista o que enviaria
    python alerts.py --test      # envia uma mensagem de teste para cada canal configurado
"""

from __future__ import annotations

import argparse
import os

import httpx

from db import all_active_listings, connect, init_db, insert_event, now_iso
from logging_setup import get_logger, kv

log = get_logger("alerts")

SCORE_THRESHOLD = 80
DISCOUNT_THRESHOLD = 30.0

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


def _format_message(listing) -> str:
    title = listing["current_title"] or "(sem título)"
    price = listing["current_price"] or "?"
    currency = listing["current_currency"] or ""
    discount = listing["discount_percentage"]
    emv = listing["estimated_market_value"]
    score = listing["opportunity_score"] or 0

    lines = [
        f"🔔 Oportunidade score={score}",
        f"{title}",
        f"preço: {price} {currency}".strip(),
    ]
    if discount is not None:
        lines.append(f"desconto: {discount:.0f}%")
    if emv is not None:
        lines.append(f"valor estimado: {emv:.0f}")
    lines.append(listing["url"])
    return "\n".join(lines)


# --- canais ----------------------------------------------------------------

def send_telegram(msg: str) -> bool | None:
    """Retorna True/False se configurado; None se não há config."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat:
        return None
    url = TELEGRAM_API.format(token=token)
    try:
        r = httpx.post(url, json={"chat_id": chat, "text": msg,
                                  "disable_web_page_preview": False},
                       timeout=10)
        ok = r.status_code == 200
        if not ok:
            log.warning(kv(event="telegram_http", code=r.status_code,
                          body=r.text[:200]))
        return ok
    except httpx.RequestError as e:
        log.error(kv(event="telegram_error", error=type(e).__name__))
        return False


def send_discord(msg: str) -> bool | None:
    url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not url:
        return None
    try:
        r = httpx.post(url, json={"content": msg}, timeout=10)
        ok = 200 <= r.status_code < 300
        if not ok:
            log.warning(kv(event="discord_http", code=r.status_code,
                          body=r.text[:200]))
        return ok
    except httpx.RequestError as e:
        log.error(kv(event="discord_error", error=type(e).__name__))
        return False


CHANNELS = {
    "telegram": send_telegram,
    "discord": send_discord,
}


# --- logic -----------------------------------------------------------------

def should_alert(listing) -> bool:
    score = listing["opportunity_score"] or 0
    discount = listing["discount_percentage"] or 0
    return score >= SCORE_THRESHOLD or discount >= DISCOUNT_THRESHOLD


def _already_alerted(conn, listing_id: str, channel: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM events WHERE listing_id = ? AND event_type = 'alert_sent' "
        "AND old_value = ? LIMIT 1",
        (listing_id, channel),
    ).fetchone()
    return row is not None


def scan_and_alert(dry_run: bool = False) -> dict:
    init_db()
    sent = {"telegram": 0, "discord": 0, "skipped_dedup": 0,
            "skipped_unconfigured": 0, "candidates": 0}

    with connect() as conn:
        listings = all_active_listings(conn)
        for l in listings:
            if not should_alert(l):
                continue
            sent["candidates"] += 1
            msg = _format_message(l)

            for channel, fn in CHANNELS.items():
                if _already_alerted(conn, l["id"], channel):
                    sent["skipped_dedup"] += 1
                    continue
                if dry_run:
                    log.info(kv(event="alert_dry_run",
                                listing=l["id"], channel=channel))
                    continue
                result = fn(msg)
                if result is None:
                    sent["skipped_unconfigured"] += 1
                    continue
                if result:
                    insert_event(conn, l["id"], now_iso(),
                                 "alert_sent", channel, "ok")
                    sent[channel] += 1
                    log.info(kv(event="alert_sent",
                                listing=l["id"], channel=channel))

    return sent


def send_test_messages() -> dict:
    """Envia uma mensagem de teste em cada canal configurado. Não toca DB."""
    msg = "🧪 Test message from FB Marketplace Audit"
    results = {}
    for channel, fn in CHANNELS.items():
        results[channel] = fn(msg)
    return results


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--test", action="store_true",
                    help="envia mensagem de teste aos canais configurados")
    args = ap.parse_args()

    if args.test:
        results = send_test_messages()
        for ch, r in results.items():
            state = "ok" if r else ("not configured" if r is None else "failed")
            print(f"  {ch:10s} {state}")
        return 0

    result = scan_and_alert(dry_run=args.dry_run)
    for k, v in result.items():
        print(f"  {k:25s} {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
