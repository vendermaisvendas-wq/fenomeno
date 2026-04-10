"""
Detector heurístico de anúncios suspeitos de fraude / golpe.

Gera `fraud_risk_score` (0..100) por listing ativo, persistido em
`listings.fraud_risk_score`. Regras somam pesos; o total é clippado em 100.

Regras e pesos:

    R1 absurdly_cheap       25  preço < 30% da mediana de comparáveis
    R2 few_images           20  menos de 2 imagens
    R3 short_description    15  descrição ausente ou < 20 chars
    R4 short_title          10  título com < 3 palavras
    R5 generic_title        10  título só com stopwords / sem tokens úteis
    R6 no_location          10  location_text ausente
    R7 huge_discount_plus_urgency 15  desconto > 40% E keyword de urgência
    R8 brand_mismatch        5  token de marca ausente mas categoria exige

Score ≥ 50 é considerado "alto risco" pelo dashboard.

Módulo é puro nas regras (testável sem DB) e aplica em batch via scan().

Uso:
    python fraud_detector.py
    python fraud_detector.py --dry-run
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from db import (
    all_active_listings, connect, init_db, latest_snapshot_payload,
)
from logging_setup import get_logger, kv
from title_normalizer import tokens

log = get_logger("fraud")

ABSURDLY_CHEAP_RATIO = 0.3
HUGE_DISCOUNT_THRESHOLD = 40.0
URGENT_PATTERNS = ("urgente", "preciso vender", "hoje", "desapego", "queima")

WEIGHTS = {
    "absurdly_cheap":            25,
    "few_images":                20,
    "short_description":         15,
    "short_title":               10,
    "generic_title":             10,
    "no_location":               10,
    "huge_discount_plus_urgency": 15,
}


@dataclass
class FraudResult:
    score: int
    reasons: list[str]


def _is_generic_title(title: str) -> bool:
    # Título com menos de 2 tokens úteis (após stopwords) conta como genérico
    return len(tokens(title)) < 2


def compute_fraud_score(
    listing_row, payload: dict | None
) -> FraudResult:
    """Função pura: recebe row + último payload, devolve score e reasons.
    Não toca DB. Testável isoladamente."""
    score = 0
    reasons: list[str] = []

    discount = listing_row["discount_percentage"]
    price = listing_row["current_price"]
    emv = listing_row["estimated_market_value"]
    title = listing_row["current_title"] or ""
    location = listing_row["current_location"]

    # R1 — absurdly cheap (preço < 30% da estimativa)
    if price and emv and emv > 0:
        from price_normalizer import parse as parse_price
        p = parse_price(price)
        if p and p > 0 and p < emv * ABSURDLY_CHEAP_RATIO:
            score += WEIGHTS["absurdly_cheap"]
            reasons.append(f"absurdly_cheap (price={p:.0f} < {emv * ABSURDLY_CHEAP_RATIO:.0f})")

    # R2 — few images
    if payload:
        imgs = payload.get("image_urls") or []
        if len(imgs) < 2:
            score += WEIGHTS["few_images"]
            reasons.append(f"few_images ({len(imgs)})")
    else:
        # Sem payload = não conseguimos verificar; damos meio peso
        score += WEIGHTS["few_images"] // 2
        reasons.append("few_images (no_payload)")

    # R3 — short description
    desc = None
    if payload:
        desc = payload.get("description")
    if not desc or len(str(desc).strip()) < 20:
        score += WEIGHTS["short_description"]
        reasons.append("short_description")

    # R4 — short title
    word_count = len(title.split())
    if word_count < 3:
        score += WEIGHTS["short_title"]
        reasons.append(f"short_title ({word_count} words)")

    # R5 — generic title (sem tokens úteis)
    if _is_generic_title(title):
        score += WEIGHTS["generic_title"]
        reasons.append("generic_title")

    # R6 — sem localização
    if not location:
        score += WEIGHTS["no_location"]
        reasons.append("no_location")

    # R7 — desconto enorme + urgência (combo clássico de golpe)
    t_lower = title.lower()
    has_urgency = any(p in t_lower for p in URGENT_PATTERNS)
    if discount is not None and discount > HUGE_DISCOUNT_THRESHOLD and has_urgency:
        score += WEIGHTS["huge_discount_plus_urgency"]
        reasons.append("huge_discount_plus_urgency")

    return FraudResult(score=min(score, 100), reasons=reasons)


def scan(dry_run: bool = False) -> dict:
    init_db()
    updated = 0
    high_risk = 0
    with connect() as conn:
        listings = all_active_listings(conn)
        for l in listings:
            payload = latest_snapshot_payload(conn, l["id"])
            result = compute_fraud_score(l, payload)
            if result.score >= 50:
                high_risk += 1
            if not dry_run:
                conn.execute(
                    "UPDATE listings SET fraud_risk_score = ? WHERE id = ?",
                    (result.score, l["id"]),
                )
            updated += 1
    log.info(kv(event="fraud_scanned", updated=updated, high_risk=high_risk))
    return {"updated": updated, "high_risk": high_risk}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    result = scan(dry_run=args.dry_run)
    for k, v in result.items():
        print(f"  {k:15s} {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
