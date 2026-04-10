"""
Detector de anúncios "muito recentes" — flag binária `very_recent_listing`
em `listings`.

Diferença vs. fresh_opportunity_detector (v7):
    fresh_opportunity_detector → score 0..100, foca em deal-quality
    recent_listing_detector    → flag 0/1, foca em "acabou de aparecer"

Critérios para very_recent_listing = 1:
    1. first_seen_at dentro da janela (default: 60 min)
    2. count(price_history) <= 1   (sem oscilação de preço — sinal de novo)
    3. count(events) baixo (≤ 3)   (poucos eventos = pouca história)

Reset: listings que estavam flagged mas agora estão fora da janela são
limpos para 0. A flag é volátil por design — só representa o estado AGORA.

Uso:
    python recent_listing_detector.py                # roda detect, grava
    python recent_listing_detector.py --window 30    # janela 30 min
    python recent_listing_detector.py --dry-run
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from db import connect, init_db
from logging_setup import get_logger, kv

log = get_logger("recent_listing")

DEFAULT_WINDOW_MINUTES = 60
MAX_PRICE_HISTORY_POINTS = 1
MAX_EVENTS = 3


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


def is_very_recent(
    listing_row, ph_count: int, ev_count: int,
    now: datetime, window_min: float,
) -> bool:
    """Função pura: dadas as contagens já calculadas, decide a flag."""
    fs = _parse_iso(listing_row["first_seen_at"])
    if fs is None:
        return False
    age_min = (now - fs).total_seconds() / 60.0
    if age_min > window_min:
        return False
    if ph_count > MAX_PRICE_HISTORY_POINTS:
        return False
    if ev_count > MAX_EVENTS:
        return False
    return True


def detect(window_minutes: float = DEFAULT_WINDOW_MINUTES,
           dry_run: bool = False) -> dict:
    init_db()
    now = datetime.now(timezone.utc)
    threshold = (now - timedelta(minutes=window_minutes)).isoformat(timespec="seconds")

    with connect() as conn:
        candidates = conn.execute(
            """
            SELECT id, first_seen_at
              FROM listings
             WHERE is_removed = 0
               AND first_seen_at >= ?
            """,
            (threshold,),
        ).fetchall()

        flagged = 0
        for row in candidates:
            ph_count = conn.execute(
                "SELECT COUNT(*) FROM price_history WHERE listing_id = ?",
                (row["id"],),
            ).fetchone()[0]
            ev_count = conn.execute(
                "SELECT COUNT(*) FROM events WHERE listing_id = ?",
                (row["id"],),
            ).fetchone()[0]

            recent = is_very_recent(row, ph_count, ev_count, now, window_minutes)
            if recent:
                if not dry_run:
                    conn.execute(
                        "UPDATE listings SET very_recent_listing = 1 WHERE id = ?",
                        (row["id"],),
                    )
                flagged += 1

        # Reset os que ficaram velhos
        if not dry_run:
            cur = conn.execute(
                "UPDATE listings SET very_recent_listing = 0 "
                "WHERE very_recent_listing = 1 AND first_seen_at < ?",
                (threshold,),
            )
            reset = cur.rowcount
        else:
            reset = 0

    result = {
        "candidates_in_window": len(candidates),
        "flagged": flagged,
        "reset_to_zero": reset,
        "window_minutes": window_minutes,
    }
    log.info(kv(event="very_recent_detected", **result))
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--window", type=float, default=DEFAULT_WINDOW_MINUTES)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    result = detect(window_minutes=args.window, dry_run=args.dry_run)
    for k, v in result.items():
        print(f"  {k:25s} {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
