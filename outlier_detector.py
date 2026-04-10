"""
Detector de outliers de preço usando IQR (interquartile range).

Para cada grupo de comparáveis (baseado em marca, ou em token principal
quando não há marca), calcula Q1, Q3, IQR e marca como outlier todo item
com preço abaixo de Q1 − 1.5·IQR ou acima de Q3 + 1.5·IQR.

Objetivo: proteger o market_value de contaminação por anúncios absurdos
(ex: digitação errada "R$ 1,00" ou carro a "R$ 999.999.999").

Flag persistida em `listings.price_outlier` (0/1).

Uso:
    python outlier_detector.py
    python outlier_detector.py --dry-run
"""

from __future__ import annotations

import argparse
import statistics
from collections import defaultdict

from db import connect, init_db
from logging_setup import get_logger, kv
from market_value import PricedItem, _load_priced_items, percentile

log = get_logger("outliers")

IQR_MULTIPLIER = 1.5
MIN_GROUP_FOR_IQR = 5


def _group_key(item: PricedItem) -> str:
    """Mesma marca é o agrupamento primário. Sem marca, usa primeiro token
    lexicográfico — hack simples mas estável. Itens sem tokens vão para um
    bucket comum e nunca são marcados como outlier (amostra ruim)."""
    if item.brand:
        return f"brand:{item.brand}"
    if item.tokens:
        return f"tok:{sorted(item.tokens)[0]}"
    return "_none"


def detect_outliers(dry_run: bool = False) -> dict:
    init_db()
    with connect() as conn:
        # Carrega TUDO (inclusive já-marcados, para poder desmarcar se o preço
        # mudou ou o grupo mudou)
        items = _load_priced_items(conn, exclude_outliers=False)

        groups: dict[str, list[PricedItem]] = defaultdict(list)
        for it in items:
            groups[_group_key(it)].append(it)

        outlier_ids: set[str] = set()
        group_stats: dict[str, tuple[float, float]] = {}

        for key, group in groups.items():
            if len(group) < MIN_GROUP_FOR_IQR:
                continue  # amostra pequena demais para IQR ser confiável
            prices = sorted(p.price for p in group)
            q1 = percentile(prices, 25)
            q3 = percentile(prices, 75)
            iqr = q3 - q1
            if iqr <= 0:
                continue
            lo = q1 - IQR_MULTIPLIER * iqr
            hi = q3 + IQR_MULTIPLIER * iqr
            group_stats[key] = (lo, hi)
            for p in group:
                if p.price < lo or p.price > hi:
                    outlier_ids.add(p.id)

        if not dry_run:
            # Resetar todos e depois marcar os outliers atuais
            conn.execute("UPDATE listings SET price_outlier = 0 WHERE is_removed = 0")
            if outlier_ids:
                placeholders = ",".join("?" * len(outlier_ids))
                conn.execute(
                    f"UPDATE listings SET price_outlier = 1 WHERE id IN ({placeholders})",
                    tuple(outlier_ids),
                )

        result = {
            "items": len(items),
            "groups_analyzed": len(group_stats),
            "outliers": len(outlier_ids),
            "outlier_rate": (
                len(outlier_ids) / len(items) if items else 0.0
            ),
        }
        log.info(kv(event="outliers_detected", **result))
        return result


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    result = detect_outliers(dry_run=args.dry_run)
    for k, v in result.items():
        if isinstance(v, float):
            print(f"  {k:25s} {v:.4f}")
        else:
            print(f"  {k:25s} {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
