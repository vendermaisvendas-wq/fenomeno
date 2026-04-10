"""
Score de liquidez: 0..100 estimando a probabilidade do anúncio vender rápido.

Sem sklearn: usamos combinação linear z-score sobre features conhecidas, com
pesos calibrados pela correlação observada com "fast removal" (< 7 dias).
Simples, transparente, sem dependência.

Features e peso default:
    1. discount vs mercado        (30)  — desconto maior ⇒ mais líquido
    2. opportunity_score          (25)  — score já encapsula vários sinais
    3. tamanho da descrição       (10)  — anúncios muito curtos vendem mal
    4. cluster size               (10)  — item comum (cluster grande) vende mais rápido
    5. velocidade do token        (25)  — mediana de dias dos vendidos na mesma keyword

Pesos fixos por enquanto. Calibração dinâmica semelhante ao score_optimizer
fica como TODO (não exposto na CLI ainda).

Persistência: `listings.liquidity_score` (INTEGER 0..100).

Uso:
    python liquidity_model.py
    python liquidity_model.py --dry-run
"""

from __future__ import annotations

import argparse
import math
from dataclasses import dataclass

from db import all_active_listings, connect, init_db, latest_snapshot_payload
from logging_setup import get_logger, kv
from sales_velocity import compute_by_token
from title_normalizer import tokens

log = get_logger("liquidity")

WEIGHTS = {
    "discount":       30.0,
    "opp_score":      25.0,
    "desc_len":       10.0,
    "cluster_size":   10.0,
    "token_velocity": 25.0,
}

# Saturation points: quando o sinal atinge este valor, contribui peso máximo
SAT_DISCOUNT_PCT = 40.0   # >= 40% → max
SAT_OPP_SCORE = 80.0      # >= 80 → max
SAT_DESC_LEN = 200        # >= 200 chars → max
SAT_CLUSTER_SIZE = 10     # >= 10 membros → max
FAST_VELOCITY_DAYS = 7    # <= 7 dias → max liquidez por velocidade


@dataclass
class Signal:
    name: str
    value: float
    contribution: float


def _scale(value: float, saturation: float) -> float:
    """Escala linear [0..1] saturada em `saturation`."""
    if value <= 0:
        return 0.0
    return min(value / saturation, 1.0)


def _velocity_index(velocity_by_token: dict) -> dict[str, float]:
    """Converte a lista de VelocityStats em dict[token] → scaled_velocity
    (1.0 para muito rápido, 0 para muito lento). Saturação: <= 7 dias."""
    out = {}
    for vs in velocity_by_token:
        tok = vs.scope.replace("token:", "")
        # Inverte: menor median_days = maior liquidez
        if vs.median_days <= 0:
            out[tok] = 1.0
        elif vs.median_days <= FAST_VELOCITY_DAYS:
            out[tok] = 1.0
        else:
            # Decai linearmente: 30 dias → 0
            decay = max(0.0, 1.0 - (vs.median_days - FAST_VELOCITY_DAYS) / 30.0)
            out[tok] = decay
    return out


def compute_liquidity(
    listing_row, payload: dict | None,
    cluster_sizes: dict[int, int], velocity: dict[str, float],
) -> tuple[int, list[Signal]]:
    """Pura: recebe dados já agregados, devolve (score 0..100, signals)."""
    signals: list[Signal] = []
    total = 0.0

    # 1. Discount
    d = listing_row["discount_percentage"]
    if d is not None and d > 0:
        s = _scale(d, SAT_DISCOUNT_PCT)
        c = s * WEIGHTS["discount"]
        total += c
        signals.append(Signal("discount", d, c))

    # 2. Opportunity score (já é 0..100)
    sc = listing_row["opportunity_score"]
    if sc is not None and sc > 0:
        s = _scale(sc, SAT_OPP_SCORE)
        c = s * WEIGHTS["opp_score"]
        total += c
        signals.append(Signal("opp_score", sc, c))

    # 3. Description length (payload)
    desc_len = 0
    if payload and isinstance(payload, dict):
        desc = payload.get("description") or ""
        desc_len = len(desc)
    s = _scale(desc_len, SAT_DESC_LEN)
    c = s * WEIGHTS["desc_len"]
    total += c
    signals.append(Signal("desc_len", desc_len, c))

    # 4. Cluster size (produtos populares vendem mais rápido)
    # Sem cluster_id conhecido → não há sinal, contribuição zero.
    cid = listing_row["cluster_id"]
    if cid is not None:
        cs = cluster_sizes.get(cid, 1)
        s = _scale(cs, SAT_CLUSTER_SIZE)
        c = s * WEIGHTS["cluster_size"]
        total += c
        signals.append(Signal("cluster_size", cs, c))

    # 5. Token velocity: maior liquidez entre os tokens do título
    title = listing_row["current_title"] or ""
    item_toks = tokens(title)
    best_velocity = 0.0
    for tok in item_toks:
        v = velocity.get(tok)
        if v is not None and v > best_velocity:
            best_velocity = v
    c = best_velocity * WEIGHTS["token_velocity"]
    total += c
    signals.append(Signal("token_velocity", best_velocity, c))

    score = int(round(min(total, 100.0)))
    return score, signals


def score_all(dry_run: bool = False) -> dict:
    init_db()
    with connect() as conn:
        listings = all_active_listings(conn)
        # Pré-compute: cluster sizes
        cluster_sizes: dict[int, int] = {}
        for r in conn.execute(
            "SELECT cluster_id, COUNT(*) as n FROM listings "
            "WHERE is_removed = 0 AND cluster_id IS NOT NULL "
            "GROUP BY cluster_id"
        ).fetchall():
            cluster_sizes[r["cluster_id"]] = r["n"]

    # Velocity por token (caro — roda uma vez)
    velocity_stats = compute_by_token(limit=200)
    velocity = _velocity_index(velocity_stats)

    updates: list[tuple[int, str]] = []
    with connect() as conn:
        for l in listings:
            payload = latest_snapshot_payload(conn, l["id"])
            score, _ = compute_liquidity(l, payload, cluster_sizes, velocity)
            updates.append((score, l["id"]))

        if not dry_run and updates:
            conn.executemany(
                "UPDATE listings SET liquidity_score = ? WHERE id = ?",
                updates,
            )

    result = {
        "listings_scored": len(updates),
        "velocity_tokens_known": len(velocity),
        "clusters_tracked": len(cluster_sizes),
    }
    log.info(kv(event="liquidity_computed", **result))
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    result = score_all(dry_run=args.dry_run)
    for k, v in result.items():
        print(f"  {k:25s} {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
