"""
Análise de padrões de vendedor e score de confiabilidade.

Fonte do nome do vendedor: `listings.current_seller`, populado pelo monitor
a partir de `Listing.seller_name`. Quando o FB não expôe (sem login),
`seller_name` vem None e o listing não entra na análise.

Métricas por vendedor, persistidas em `seller_stats`:

    total_listings          Total de anúncios que vimos deste vendedor
    active_listings         Ativos agora
    removed_listings        Já removidos
    duplicate_count         Quantos anúncios deste vendedor caem num
                            duplicate_group_id onde há outros anúncios dele
    avg_price               Preço médio
    avg_opportunity         Score médio
    reliability_score       0..100 (100 = mais confiável)

Heurística de reliability:
    começa em 100, penaliza:
    - dup_ratio alto (> 30% dos anúncios duplicados entre si)  -20
    - volume muito alto (>50 listings)                         -10 (flooder)
    - fraud_score médio alto (>50)                             -25
    - muitos "novos" e removidos rápido (churn)                -15

Também grava o score final em `listings.seller_reliability_score` para
cada listing daquele vendedor — permite ordenação no dashboard.

Uso:
    python seller_patterns.py
    python seller_patterns.py --top 20        # mostra ranking
    python seller_patterns.py --dry-run
"""

from __future__ import annotations

import argparse
import statistics
from collections import defaultdict
from dataclasses import dataclass

from db import connect, init_db, now_iso
from logging_setup import get_logger, kv
from price_normalizer import parse as parse_price

log = get_logger("sellers")


@dataclass
class SellerMetrics:
    seller: str
    total: int
    active: int
    removed: int
    dup_count: int
    avg_price: float | None
    avg_opp: float | None
    avg_fraud: float | None
    reliability: int


FLOODER_THRESHOLD = 50
HIGH_DUP_RATIO = 0.30
HIGH_FRAUD_AVG = 50.0
CHURN_RATIO = 0.70   # fração removida muito rápida


def _compute_reliability(
    total: int, dup_count: int, fraud_avg: float | None,
    removed: int,
) -> int:
    score = 100
    if total > 0:
        dup_ratio = dup_count / total
        if dup_ratio > HIGH_DUP_RATIO:
            score -= 20
    if total > FLOODER_THRESHOLD:
        score -= 10
    if fraud_avg is not None and fraud_avg > HIGH_FRAUD_AVG:
        score -= 25
    if total > 5 and removed / total > CHURN_RATIO:
        score -= 15
    return max(0, min(100, score))


def _analyze() -> list[SellerMetrics]:
    init_db()
    results: list[SellerMetrics] = []

    with connect() as conn:
        rows = conn.execute(
            """
            SELECT current_seller, id, current_price, opportunity_score,
                   fraud_risk_score, is_removed, duplicate_group_id
              FROM listings
             WHERE current_seller IS NOT NULL AND current_seller != ''
            """
        ).fetchall()

    by_seller: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_seller[r["current_seller"]].append(dict(r))

    for seller, listings in by_seller.items():
        total = len(listings)
        active = sum(1 for l in listings if not l["is_removed"])
        removed = total - active

        # dup_count: quantos estão num grupo cujo grupo contém > 1 listing deste mesmo seller
        groups: dict[int, int] = defaultdict(int)
        for l in listings:
            g = l["duplicate_group_id"]
            if g is not None:
                groups[g] += 1
        dup_count = sum(n for n in groups.values() if n > 1)

        prices = [parse_price(l["current_price"]) for l in listings]
        prices = [p for p in prices if p is not None]
        avg_price = round(statistics.fmean(prices), 2) if prices else None

        opps = [l["opportunity_score"] for l in listings if l["opportunity_score"] is not None]
        avg_opp = round(statistics.fmean(opps), 1) if opps else None

        frauds = [l["fraud_risk_score"] for l in listings if l["fraud_risk_score"] is not None]
        avg_fraud = round(statistics.fmean(frauds), 1) if frauds else None

        reliability = _compute_reliability(total, dup_count, avg_fraud, removed)
        results.append(SellerMetrics(
            seller=seller, total=total, active=active, removed=removed,
            dup_count=dup_count, avg_price=avg_price, avg_opp=avg_opp,
            avg_fraud=avg_fraud, reliability=reliability,
        ))

    return results


def scan(dry_run: bool = False) -> dict:
    metrics = _analyze()
    if not metrics:
        log.info(kv(event="no_sellers_seen"))
        return {"sellers": 0, "updated": 0}

    computed_at = now_iso()
    with connect() as conn:
        if not dry_run:
            # Reseta tabela e reescreve
            conn.execute("DELETE FROM seller_stats")
            for m in metrics:
                conn.execute(
                    """
                    INSERT INTO seller_stats
                      (seller_name, total_listings, active_listings, removed_listings,
                       duplicate_count, avg_price, avg_opportunity, reliability_score,
                       computed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (m.seller, m.total, m.active, m.removed, m.dup_count,
                     m.avg_price, m.avg_opp, m.reliability, computed_at),
                )
            # Denormalizar o score nas listings para ordenação rápida
            for m in metrics:
                conn.execute(
                    "UPDATE listings SET seller_reliability_score = ? "
                    "WHERE current_seller = ?",
                    (m.reliability, m.seller),
                )

    result = {
        "sellers": len(metrics),
        "updated": len(metrics) if not dry_run else 0,
    }
    log.info(kv(event="seller_patterns_scanned", **result))
    return result


def print_top(limit: int = 20) -> None:
    init_db()
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM seller_stats ORDER BY total_listings DESC LIMIT ?",
            (limit,),
        ).fetchall()
    if not rows:
        print("(no seller stats — rode sem --top primeiro)")
        return
    header = f"{'seller':30s} {'total':>6s} {'active':>7s} {'dup':>5s} {'rely':>5s}"
    print(header)
    print("-" * len(header))
    for r in rows:
        print(
            f"{(r['seller_name'] or '')[:30]:30s} "
            f"{r['total_listings']:6d} {r['active_listings']:7d} "
            f"{r['duplicate_count']:5d} {r['reliability_score']:5d}"
        )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--top", type=int, default=0,
                    help="mostra ranking ao invés de recalcular")
    args = ap.parse_args()

    if args.top > 0:
        print_top(args.top)
        return 0

    result = scan(dry_run=args.dry_run)
    print(f"sellers: {result['sellers']}  updated: {result['updated']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
