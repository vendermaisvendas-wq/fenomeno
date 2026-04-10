"""
Estimador de probabilidade de oportunidade real.

Persiste `listings.opportunity_probability` ∈ [0, 1] baseado em sinais
existentes (discount, liquidity, velocity por token, fraud, outlier).

⚠ NÃO É MODELO TREINADO. É HEURÍSTICA CALIBRADA.

Sem ground truth confiável de "este foi vendido por X" (todos os sinais
de venda são proxies — ver sales_velocity.py docstring), não dá pra treinar
um modelo supervisionado responsavelmente. Os pesos abaixo são calibrados
manualmente por intuição de produto. Eles devem ser ajustados via
score_optimizer.py-like analysis quando houver dados de outcome reais.

Combinação:
    f1 = discount_normalized       (peso 0.35)
    f2 = liquidity_score / 100     (peso 0.20)
    f3 = best_token_velocity       (peso 0.20)
    f4 = NOT high_fraud            (peso 0.15)
    f5 = NOT outlier               (peso 0.10)

Saída: f1*w1 + f2*w2 + f3*w3 + f4*w4 + f5*w5  ∈ [0, 1]

Uso:
    python opportunity_predictor.py
    python opportunity_predictor.py --dry-run
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from db import all_active_listings, connect, init_db
from logging_setup import get_logger, kv

log = get_logger("opp_predictor")

WEIGHTS = {
    "discount":    0.35,
    "liquidity":   0.20,
    "velocity":    0.20,
    "not_fraud":   0.15,
    "not_outlier": 0.10,
}
DISCOUNT_SATURATION = 50.0   # >= 50% → 1.0


def compute_probability(listing_row, token_velocity_index: dict[str, float] | None = None) -> float:
    """Pure: row + opcional índice de velocidade por token. Devolve prob."""
    discount = listing_row["discount_percentage"] or 0.0
    liquidity = listing_row["liquidity_score"] or 0
    fraud = listing_row["fraud_risk_score"] or 0
    outlier = listing_row["price_outlier"] or 0

    f1 = max(0.0, min(1.0, discount / DISCOUNT_SATURATION))
    f2 = max(0.0, min(1.0, liquidity / 100.0))

    f3 = 0.0
    if token_velocity_index and listing_row["current_title"]:
        from title_normalizer import tokens
        toks = tokens(listing_row["current_title"])
        velocities = [token_velocity_index.get(t, 0.0) for t in toks]
        if velocities:
            f3 = max(velocities)

    f4 = 1.0 if fraud < 50 else 0.0
    f5 = 1.0 if not outlier else 0.0

    raw = (
        f1 * WEIGHTS["discount"]
        + f2 * WEIGHTS["liquidity"]
        + f3 * WEIGHTS["velocity"]
        + f4 * WEIGHTS["not_fraud"]
        + f5 * WEIGHTS["not_outlier"]
    )
    return round(min(1.0, max(0.0, raw)), 4)


def _build_velocity_index() -> dict[str, float]:
    """Lê market_density e converte velocity em [0, 1] (1 = vende rápido).
    Saturação: 7 dias = max liquidez."""
    try:
        with connect() as conn:
            rows = conn.execute(
                "SELECT token, avg_velocity_days FROM market_density "
                "WHERE avg_velocity_days IS NOT NULL"
            ).fetchall()
    except Exception:
        return {}
    out = {}
    for r in rows:
        d = r["avg_velocity_days"]
        if d is None or d <= 0:
            out[r["token"]] = 1.0
        elif d <= 7:
            out[r["token"]] = 1.0
        else:
            out[r["token"]] = max(0.0, 1.0 - (d - 7) / 30.0)
    return out


def predict_all(dry_run: bool = False) -> dict:
    init_db()
    velocity_index = _build_velocity_index()
    updates: list[tuple[float, str]] = []

    with connect() as conn:
        listings = all_active_listings(conn)
        for l in listings:
            prob = compute_probability(l, velocity_index)
            updates.append((prob, l["id"]))

        if not dry_run and updates:
            conn.executemany(
                "UPDATE listings SET opportunity_probability = ? WHERE id = ?",
                updates,
            )

    high = sum(1 for p, _ in updates if p >= 0.7)
    medium = sum(1 for p, _ in updates if 0.4 <= p < 0.7)
    log.info(kv(event="opportunity_predicted",
                total=len(updates), high=high, medium=medium))
    return {
        "total": len(updates),
        "high_probability": high,
        "medium_probability": medium,
        "velocity_tokens": len(velocity_index),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    result = predict_all(dry_run=args.dry_run)
    for k, v in result.items():
        print(f"  {k:25s} {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
