"""
Reconstrução da timeline completa de um listing.

Combina três fontes:
    1. Campos denormalizados em `listings` (first_seen_at, removed_at, reappeared_at)
    2. Tabela `events` (price_change, title_change, opportunity_flag, etc.)
    3. Tabela `price_history` (ponto-a-ponto de preço)

Resultado: lista ordenada de `TimelineEntry(at, kind, description, details)`
pronta para serializar em JSON ou renderizar em HTML.

Eventos tipados:
    listing_created
    price_changed
    title_changed
    opportunity_flag
    removed
    reappeared
    alert_sent
    new_opportunity
    fresh_opportunity
    repost_detected
    status_change
    parser_break (só para __system__)

Uso:
    python listing_timeline.py 2015275022700246
    python listing_timeline.py 2015275022700246 --json
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass, field

from db import (
    connect, events_for, init_db, listing_by_id, price_history_for,
)
from logging_setup import get_logger

log = get_logger("timeline")


@dataclass
class TimelineEntry:
    at: str
    kind: str
    description: str
    details: dict = field(default_factory=dict)


# Descrições humanas por event_type
DESCRIPTIONS = {
    "first_seen":        "anúncio descoberto pelo monitor",
    "price_change":      "preço alterado",
    "title_change":      "título alterado",
    "status_change":     "status mudou",
    "removed":           "anúncio removido",
    "reappeared":        "anúncio voltou após remoção",
    "opportunity_flag":  "flag de oportunidade",
    "new_opportunity":   "nova oportunidade detectada",
    "fresh_opportunity": "oportunidade fresca (<30min)",
    "repost_detected":   "anúncio republicado",
    "alert_sent":        "alerta enviado",
    "parser_break":      "parser regrediu (sistema)",
}


def build_timeline(listing_id: str) -> list[TimelineEntry]:
    init_db()
    entries: list[TimelineEntry] = []

    with connect() as conn:
        listing = listing_by_id(conn, listing_id)
        if listing is None:
            return []

        # 1. created (derivado de first_seen_at)
        entries.append(TimelineEntry(
            at=listing["first_seen_at"],
            kind="listing_created",
            description="listing criado (primeiro fetch)",
            details={
                "source": listing["source"],
                "initial_title": listing["current_title"],
            },
        ))

        # 2. eventos da tabela events (inclui price_change, title_change,
        #    removed, reappeared, opportunity_flag, alert_sent, etc.)
        for ev in events_for(conn, listing_id):
            if ev["event_type"] == "first_seen":
                continue  # já coberto acima
            desc = DESCRIPTIONS.get(ev["event_type"], ev["event_type"])
            entries.append(TimelineEntry(
                at=ev["at"],
                kind=ev["event_type"],
                description=desc,
                details={
                    "old": ev["old_value"],
                    "new": ev["new_value"],
                },
            ))

        # 3. price_history como pontos independentes (complementar aos events
        #    — útil para anúncios onde o price_change ainda não foi emitido
        #    mas temos snapshots históricos).
        already_at_prices: set[str] = {
            e.at for e in entries if e.kind == "price_changed"
        }
        for ph in price_history_for(conn, listing_id):
            if ph["recorded_at"] in already_at_prices:
                continue
            entries.append(TimelineEntry(
                at=ph["recorded_at"],
                kind="price_point",
                description=f"preço registrado: {ph['price_raw'] or ph['price']}",
                details={
                    "price": ph["price"],
                    "currency": ph["currency"],
                    "raw": ph["price_raw"],
                },
            ))

    # Ordenar cronologicamente
    entries.sort(key=lambda e: (e.at, e.kind))
    return entries


def timeline_json(listing_id: str) -> str:
    entries = build_timeline(listing_id)
    return json.dumps([asdict(e) for e in entries], ensure_ascii=False, indent=2)


def print_timeline(listing_id: str) -> int:
    entries = build_timeline(listing_id)
    if not entries:
        print(f"listing {listing_id} não encontrado ou sem eventos")
        return 1

    print(f"=== Timeline {listing_id} ===")
    for e in entries:
        print(f"{e.at[:19]:19s}  [{e.kind:20s}]  {e.description}")
        for k, v in e.details.items():
            if v is None:
                continue
            s = str(v)
            if len(s) > 60:
                s = s[:57] + "..."
            print(f"{'':22s}    {k}: {s}")
    print(f"\n{len(entries)} entries")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("listing_id")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if args.json:
        print(timeline_json(args.listing_id))
        return 0
    return print_timeline(args.listing_id)


if __name__ == "__main__":
    raise SystemExit(main())
