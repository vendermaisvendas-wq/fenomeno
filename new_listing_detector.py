"""
Detecta anúncios *recém-descobertos* que tenham indícios de serem
oportunidades. Deve rodar após market_value.recompute_all() — depende de
`discount_percentage` já estar atualizado nos listings.

Critérios (precisa de pelo menos um hit forte OU dois hits fracos):

    strong:  discount_percentage >= BIG_DISCOUNT_PCT (default 20%)
    strong:  first_seen < RECENT_HOURS (default 2h) AND popular_keyword

    weak:    first_seen < RECENT_HOURS
    weak:    popular_keyword in title
    weak:    discount > 5%

Registra `event_type='new_opportunity'` com dedup por listing.

Uso:
    python new_listing_detector.py
    python new_listing_detector.py --hours 6 --dry-run
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from db import all_active_listings, connect, init_db, insert_event, now_iso
from logging_setup import get_logger, kv
from title_normalizer import tokens

log = get_logger("new_detector")

RECENT_HOURS = 2
BIG_DISCOUNT_PCT = 20.0

POPULAR_KEYWORDS = {
    "iphone", "macbook", "ipad", "playstation", "ps5", "xbox",
    "hilux", "cg", "titan", "civic", "corolla", "onix", "gol",
    "honda", "toyota", "yamaha", "bmw",
    "notebook", "dell",
}


def _parse_iso(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def is_recent(first_seen_iso: str, now: datetime, hours: float) -> bool:
    return (now - _parse_iso(first_seen_iso)) < timedelta(hours=hours)


def has_popular_keyword(title: str | None) -> bool:
    return bool(tokens(title) & POPULAR_KEYWORDS)


def _already_flagged(conn, listing_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM events WHERE listing_id = ? "
        "AND event_type = 'new_opportunity' LIMIT 1",
        (listing_id,),
    ).fetchone()
    return row is not None


def scan(recent_hours: float = RECENT_HOURS, dry_run: bool = False) -> dict:
    init_db()
    now = datetime.now(timezone.utc)
    flagged = 0
    scanned = 0

    with connect() as conn:
        listings = all_active_listings(conn)
        for l in listings:
            recent = is_recent(l["first_seen_at"], now, recent_hours)
            if not recent:
                continue  # só olhamos os recentes
            scanned += 1

            if _already_flagged(conn, l["id"]):
                continue

            title = l["current_title"] or ""
            discount = l["discount_percentage"]
            kw = has_popular_keyword(title)

            strong = (
                (discount is not None and discount >= BIG_DISCOUNT_PCT) or
                (kw and discount is not None and discount > 10)
            )
            weak_hits = sum([
                kw,
                discount is not None and discount > 5,
            ])

            if not strong and weak_hits < 2:
                continue

            reasons = []
            if discount is not None:
                reasons.append(f"discount={discount:.0f}%")
            if kw:
                reasons.append("popular_keyword")
            reasons.append(f"age<{recent_hours}h")

            if not dry_run:
                insert_event(conn, l["id"], now_iso(), "new_opportunity",
                             None, "; ".join(reasons))
            log.info(kv(event="new_opportunity", listing=l["id"], reasons=reasons))
            flagged += 1

    return {"scanned_recent": scanned, "flagged": flagged}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--hours", type=float, default=RECENT_HOURS)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    result = scan(recent_hours=args.hours, dry_run=args.dry_run)
    for k, v in result.items():
        print(f"  {k:20s} {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
