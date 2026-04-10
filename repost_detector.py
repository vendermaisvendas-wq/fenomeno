"""
Detecta quando um vendedor remove e republica o mesmo anúncio.

Reposts são um padrão importante no Marketplace:
  - vendedor "ressuscita" anúncio antigo para ganhar visibilidade
  - anúncio caro que não vendeu retorna com preço ajustado
  - scam: mesmo anúncio aparece em várias cidades

Algoritmo:
    Para cada listing REMOVIDO (A):
        procura listings NOVOS (B) onde:
          - B.current_seller == A.current_seller (ou ambos None → match só por título)
          - B.first_seen_at está dentro de REPOST_WINDOW_DAYS depois de A.removed_at
          - jaccard(tokens(A.title), tokens(B.title)) >= REPOST_TITLE_SIM
          - (opcional) preço dentro de ±REPOST_PRICE_TOL

    Incrementa `B.repost_count` em 1 para cada A que bate.

    Grava evento `repost_detected` com new_value = "origem:A.id".

Complexidade: agrupa por seller antes da comparação bilateral, O(S × k²)
onde S é número de sellers e k é anúncios por seller (muito menor que N²).

Uso:
    python repost_detector.py
    python repost_detector.py --window-days 30 --dry-run
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from db import connect, init_db, insert_event, now_iso
from logging_setup import get_logger, kv
from price_normalizer import parse as parse_price
from title_normalizer import jaccard, tokens

log = get_logger("repost_detector")

REPOST_WINDOW_DAYS = 14
REPOST_TITLE_SIM = 0.7
REPOST_PRICE_TOL = 0.15


def _parse(iso: str | None) -> datetime | None:
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _price_close(a: str | None, b: str | None, tol: float = REPOST_PRICE_TOL) -> bool:
    pa = parse_price(a)
    pb = parse_price(b)
    if pa is None or pb is None:
        return True  # sem preço de um dos lados → não penaliza
    if pa <= 0 or pb <= 0:
        return False
    lo, hi = sorted((pa, pb))
    return (hi - lo) / lo <= tol


def _is_repost(removed: dict, new: dict, window: timedelta) -> bool:
    removed_dt = _parse(removed.get("removed_at"))
    new_dt = _parse(new.get("first_seen_at"))
    if removed_dt is None or new_dt is None:
        return False
    delta = new_dt - removed_dt
    if delta <= timedelta(0) or delta > window:
        return False

    if jaccard(tokens(removed["current_title"]), tokens(new["current_title"])) < REPOST_TITLE_SIM:
        return False
    if not _price_close(removed["current_price"], new["current_price"]):
        return False
    return True


def detect_reposts(
    window_days: int = REPOST_WINDOW_DAYS, dry_run: bool = False,
) -> dict:
    init_db()
    window = timedelta(days=window_days)

    with connect() as conn:
        rows = conn.execute(
            """
            SELECT id, current_title, current_price, current_seller,
                   first_seen_at, removed_at, is_removed
              FROM listings
             WHERE current_title IS NOT NULL
            """
        ).fetchall()
        listings = [dict(r) for r in rows]

    # Bucketize por seller. Listings sem seller vão para um bucket "__no_seller__"
    # mas nele só comparamos pares com jaccard muito forte + preço igual.
    by_seller: dict[str, list[dict]] = defaultdict(list)
    for l in listings:
        key = l["current_seller"] or "__no_seller__"
        by_seller[key].append(l)

    repost_counts: dict[str, int] = defaultdict(int)
    origin_map: dict[str, list[str]] = defaultdict(list)
    pairs_tested = 0

    for seller, group in by_seller.items():
        removed_ones = [l for l in group if l["is_removed"] and l["removed_at"]]
        new_ones = [l for l in group if not l["is_removed"] and l["first_seen_at"]]

        for removed in removed_ones:
            for new in new_ones:
                pairs_tested += 1
                if _is_repost(removed, new, window):
                    repost_counts[new["id"]] += 1
                    origin_map[new["id"]].append(removed["id"])

    if not dry_run:
        with connect() as conn:
            now = now_iso()
            for lid, cnt in repost_counts.items():
                conn.execute(
                    "UPDATE listings SET repost_count = ? WHERE id = ?",
                    (cnt, lid),
                )
                for origin in origin_map[lid][:3]:  # até 3 origens por listing
                    # Dedup: não insere evento idêntico
                    existing = conn.execute(
                        "SELECT 1 FROM events WHERE listing_id = ? "
                        "AND event_type = 'repost_detected' "
                        "AND new_value = ? LIMIT 1",
                        (lid, f"origin:{origin}"),
                    ).fetchone()
                    if not existing:
                        insert_event(
                            conn, lid, now, "repost_detected",
                            None, f"origin:{origin}",
                        )

    result = {
        "sellers_analyzed": len(by_seller),
        "pairs_tested": pairs_tested,
        "listings_flagged_as_repost": len(repost_counts),
        "total_repost_events": sum(repost_counts.values()),
    }
    log.info(kv(event="reposts_detected", **result))
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--window-days", type=int, default=REPOST_WINDOW_DAYS)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    result = detect_reposts(
        window_days=args.window_days, dry_run=args.dry_run,
    )
    for k, v in result.items():
        print(f"  {k:30s} {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
