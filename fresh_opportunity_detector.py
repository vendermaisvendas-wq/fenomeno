"""
Detector de oportunidades frescas — anúncios descobertos há <30min e
com indicadores de deal.

Diferença do `new_listing_detector.py` (v4):
  - new_listing_detector: janela de 2h, critério qualitativo ("tem sinal")
  - fresh_opportunity_detector: janela de 30min, score numérico (0..100),
    combina discount + liquidity + opportunity_score em um único número

Objetivo é sinalizar o subset "compra agora ou perde" dentro do firehose
de novos anúncios.

Score (0..100):
    discount_big    → 40  (discount > 30%)
    discount_mid    → 20  (discount > 15%)
    liquidity_high  → 30  (liquidity_score >= 60)
    opp_score_high  → 20  (opportunity_score >= 70)
    popular_keyword → 10  (token popular no título)

Listings > janela recebem score 0 (não são "frescos"). Só é gravado
fresh_opportunity_score em listings frescos.

Evento `fresh_opportunity` é emitido (uma vez por listing, dedupado) quando
score >= EMIT_THRESHOLD.

Uso:
    python fresh_opportunity_detector.py
    python fresh_opportunity_detector.py --minutes 60 --threshold 50
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from db import all_active_listings, connect, init_db, insert_event, now_iso
from logging_setup import get_logger, kv
from new_listing_detector import POPULAR_KEYWORDS
from title_normalizer import tokens

log = get_logger("fresh_opps")

FRESH_MINUTES = 30
EMIT_THRESHOLD = 60

WEIGHTS = {
    "discount_big":    40,
    "discount_mid":    20,
    "liquidity_high":  30,
    "opp_score_high":  20,
    "popular_keyword": 10,
}


def _age_minutes(first_seen_at: str, now: datetime) -> float:
    try:
        dt = datetime.fromisoformat(first_seen_at)
    except ValueError:
        return float("inf")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (now - dt).total_seconds() / 60.0


def compute_fresh_score(listing, now: datetime, window_minutes: float) -> int:
    """Pura: 0 se fora da janela, senão 0..100 baseado nos sinais."""
    age = _age_minutes(listing["first_seen_at"], now)
    if age > window_minutes:
        return 0

    score = 0
    d = listing["discount_percentage"]
    if d is not None:
        if d > 30:
            score += WEIGHTS["discount_big"]
        elif d > 15:
            score += WEIGHTS["discount_mid"]

    liq = listing["liquidity_score"]
    if liq is not None and liq >= 60:
        score += WEIGHTS["liquidity_high"]

    opp = listing["opportunity_score"]
    if opp is not None and opp >= 70:
        score += WEIGHTS["opp_score_high"]

    title = listing["current_title"] or ""
    if tokens(title) & POPULAR_KEYWORDS:
        score += WEIGHTS["popular_keyword"]

    return min(score, 100)


def _already_flagged(conn, listing_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM events WHERE listing_id = ? "
        "AND event_type = 'fresh_opportunity' LIMIT 1",
        (listing_id,),
    ).fetchone()
    return row is not None


def scan(
    window_minutes: float = FRESH_MINUTES,
    threshold: int = EMIT_THRESHOLD,
    dry_run: bool = False,
) -> dict:
    init_db()
    now = datetime.now(timezone.utc)
    scored = 0
    emitted = 0
    top_score = 0

    with connect() as conn:
        listings = all_active_listings(conn)
        for l in listings:
            score = compute_fresh_score(l, now, window_minutes)
            if score == 0:
                continue  # fora da janela ou sem sinais
            scored += 1
            top_score = max(top_score, score)

            if not dry_run:
                conn.execute(
                    "UPDATE listings SET fresh_opportunity_score = ? WHERE id = ?",
                    (score, l["id"]),
                )

            if score >= threshold and not _already_flagged(conn, l["id"]):
                if not dry_run:
                    insert_event(
                        conn, l["id"], now_iso(), "fresh_opportunity",
                        None, f"score={score}",
                    )
                emitted += 1
                log.info(kv(event="fresh_opportunity",
                            listing=l["id"], score=score))

    return {
        "in_window": scored,
        "emitted": emitted,
        "top_score": top_score,
        "window_minutes": window_minutes,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--minutes", type=float, default=FRESH_MINUTES)
    ap.add_argument("--threshold", type=int, default=EMIT_THRESHOLD)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    result = scan(
        window_minutes=args.minutes,
        threshold=args.threshold,
        dry_run=args.dry_run,
    )
    for k, v in result.items():
        print(f"  {k:20s} {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
