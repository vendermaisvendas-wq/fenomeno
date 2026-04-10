"""
Simulador de investimento em oportunidades do Marketplace.

Modelo:
  Dado um capital inicial e critérios de seleção (score ≥ X, desconto ≥ Y),
  simula a "compra" de listings que passariam no filtro, estima:

    investment    = soma dos preços dos listings escolhidos, até gastar capital
    est_value     = soma dos estimated_market_value correspondentes
    gross_profit  = est_value - investment
    hit_rate      = fração dos listings "comprados" cujo id já tem evento
                    de remoção (proxy para "teria vendido")
    expected_roi  = gross_profit * hit_rate / investment * 100

Limitações declaradas:
  - Não há prova de venda; usamos "removed_at existe" como proxy otimista
  - Sem modelo de custo de manuseio/frete/tempo
  - Sem taxas ou impostos
  - Resultado é *otimista* por construção; use como teto superior

Estratégia de seleção:
  1. Filtra listings pelos critérios
  2. Ordena por opportunity_score desc, discount desc
  3. Vai escolhendo do topo até estourar o capital
  4. Para cada escolhido, verifica se foi removido → sucesso parcial

Uso:
    python deal_simulator.py --capital 50000 --min-score 70
    python deal_simulator.py --capital 20000 --min-score 60 --min-discount 20
    python deal_simulator.py --capital 10000 --keyword hilux --json
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass

from db import connect, init_db
from logging_setup import get_logger, kv
from price_normalizer import parse as parse_price

log = get_logger("simulator")


@dataclass
class SimResult:
    capital: float
    candidates_considered: int
    picks: int
    investment: float
    estimated_value: float
    gross_profit: float
    hit_rate: float
    expected_roi_pct: float
    leftover_capital: float


def simulate(
    capital: float,
    min_score: int = 60,
    min_discount: float | None = None,
    keyword: str | None = None,
    city: str | None = None,
) -> tuple[SimResult, list[dict]]:
    init_db()
    where = [
        "is_removed = 0",
        "current_price IS NOT NULL",
        "estimated_market_value IS NOT NULL",
        "COALESCE(opportunity_score, 0) >= ?",
    ]
    params: list = [min_score]

    if min_discount is not None:
        where.append("COALESCE(discount_percentage, -999) >= ?")
        params.append(min_discount)
    if keyword:
        where.append("LOWER(COALESCE(current_title, '')) LIKE ?")
        params.append(f"%{keyword.lower()}%")
    if city:
        where.append("LOWER(COALESCE(current_location, '')) LIKE ?")
        params.append(f"%{city.lower()}%")

    sql = f"""
        SELECT id, url, current_title, current_price,
               estimated_market_value, discount_percentage,
               opportunity_score, cluster_id
          FROM listings
         WHERE {' AND '.join(where)}
         ORDER BY opportunity_score DESC,
                  COALESCE(discount_percentage, 0) DESC
    """

    with connect() as conn:
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]

        # Hit-rate ground truth: listings que em algum momento tiveram 'removed'
        removed_ids = {
            r["listing_id"]
            for r in conn.execute(
                "SELECT DISTINCT listing_id FROM events "
                "WHERE event_type = 'removed'"
            ).fetchall()
        }

    candidates = len(rows)
    spent = 0.0
    est_total = 0.0
    picks: list[dict] = []
    hits = 0

    for r in rows:
        price = parse_price(r["current_price"])
        if price is None or price <= 0:
            continue
        if spent + price > capital:
            continue
        spent += price
        est_total += r["estimated_market_value"] or 0
        r["_price_parsed"] = price
        r["_removed"] = r["id"] in removed_ids
        if r["_removed"]:
            hits += 1
        picks.append(r)

    hit_rate = (hits / len(picks)) if picks else 0.0
    gross = est_total - spent
    # ROI otimista: multiplica pelo hit_rate (assume que só lucra o que vende)
    exp_roi = ((gross * hit_rate) / spent * 100.0) if spent > 0 else 0.0

    result = SimResult(
        capital=capital,
        candidates_considered=candidates,
        picks=len(picks),
        investment=round(spent, 2),
        estimated_value=round(est_total, 2),
        gross_profit=round(gross, 2),
        hit_rate=round(hit_rate, 3),
        expected_roi_pct=round(exp_roi, 2),
        leftover_capital=round(capital - spent, 2),
    )
    log.info(kv(event="simulation_done", **asdict(result)))
    return result, picks


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--capital", type=float, required=True)
    ap.add_argument("--min-score", type=int, default=60)
    ap.add_argument("--min-discount", type=float)
    ap.add_argument("--keyword")
    ap.add_argument("--city")
    ap.add_argument("--show-picks", type=int, default=0,
                    help="imprime as N primeiras escolhas")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    result, picks = simulate(
        capital=args.capital,
        min_score=args.min_score,
        min_discount=args.min_discount,
        keyword=args.keyword,
        city=args.city,
    )

    if args.json:
        print(json.dumps({
            "result": asdict(result),
            "picks": picks[:args.show_picks] if args.show_picks else [],
        }, indent=2, ensure_ascii=False, default=str))
        return 0

    for k, v in asdict(result).items():
        print(f"  {k:25s} {v}")

    if args.show_picks:
        print(f"\nTop {min(args.show_picks, len(picks))} picks:")
        for p in picks[:args.show_picks]:
            tag = "✓removed" if p.get("_removed") else " active "
            print(
                f"  [{p.get('opportunity_score'):>3}] "
                f"{tag} {p.get('current_title', '')[:50]:50s} "
                f"price={p['_price_parsed']:>10.2f}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
