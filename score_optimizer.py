"""
Otimiza os pesos do opportunity_score baseado em sinal observado.

Ground truth aproximado: "oportunidade real = listing removido rapidamente
após entrada no sistema". Não é perfeito (o motivo da remoção pode ser
ruído — ver sales_velocity.py), mas em agregado é suficiente para
calibrar pesos relativos.

Algoritmo:

  1. Para cada listing com first_seen + removed_at, marca como "fast" se
     (removed_at - first_seen) < FAST_THRESHOLD_DAYS. Caso contrário "slow".
     Listings ativos não entram (sem ground truth).

  2. Para cada sinal do compute_score atual, checamos:
     - n_fired       : quantos listings rotulados o sinal disparou
     - p_fast_given  : P(fast | signal fired)
     - base_rate     : P(fast) global
     - lift          : p_fast_given / base_rate

     Lift > 1 → o sinal realmente separa fast de slow; mantém/aumenta peso.
     Lift ~ 1 → o sinal é neutro; diminui peso.

  3. Novo peso = clamp(default_weight * lift, MIN_W, MAX_W).

  4. Persistência em `config/score_weights.json`. `opportunities.py` carrega
     esse arquivo no startup via `opportunities._load_weights()`. Se o
     arquivo não existe ou é inválido, usa defaults embutidos.

Uso:
    python score_optimizer.py                  # computa e grava
    python score_optimizer.py --dry-run        # mostra sem gravar
    python score_optimizer.py --days 14        # janela de "fast" diferente
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from db import connect, init_db
from logging_setup import get_logger, kv
from opportunities import DEFAULT_SCORE_WEIGHTS, URGENT_PATTERNS

log = get_logger("score_optimizer")

CONFIG_DIR = Path("config")
WEIGHTS_PATH = CONFIG_DIR / "score_weights.json"

FAST_THRESHOLD_DAYS = 7
MIN_SAMPLES = 20
MIN_WEIGHT = 5
MAX_WEIGHT = 60


def _parse_dt(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _load_labeled_rows(fast_days: int) -> list[tuple[dict, bool]]:
    init_db()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT id, current_title, discount_percentage, first_seen_at,
                   removed_at, is_removed
              FROM listings
             WHERE removed_at IS NOT NULL AND is_removed = 1
            """
        ).fetchall()
    labeled: list[tuple[dict, bool]] = []
    for r in rows:
        d = (_parse_dt(r["removed_at"]) - _parse_dt(r["first_seen_at"])).total_seconds() / 86400.0
        fast = d < fast_days
        labeled.append((dict(r), fast))
    return labeled


def _signal_fires(row: dict, signal: str) -> bool:
    """Reproduz o predicado de cada signal do compute_score, sem computar o
    score inteiro."""
    discount = row.get("discount_percentage")
    title = (row.get("current_title") or "").lower()

    if signal == "discount_big":
        return discount is not None and discount > 30
    if signal == "discount_mid":
        return discount is not None and 15 < discount <= 30
    if signal == "below_percentile":
        return discount is not None and discount > 25
    if signal == "urgency":
        return any(p in title for p in URGENT_PATTERNS)
    if signal == "short_desc":
        # Sem payload aqui — tratamos como unknown e não contamos
        return False
    if signal == "recent":
        # "Recent" é transient e correlaciona trivialmente com "fast";
        # manter peso default, não otimizar (evitar feedback loop)
        return False
    return False


def _lift_for(labeled: list[tuple[dict, bool]], signal: str, base_rate: float):
    fired = [fast for row, fast in labeled if _signal_fires(row, signal)]
    n = len(fired)
    if n == 0 or base_rate <= 0:
        return None, n
    p_fast_given = sum(fired) / n
    return p_fast_given / base_rate, n


def optimize(fast_days: int = FAST_THRESHOLD_DAYS, dry_run: bool = False) -> dict:
    labeled = _load_labeled_rows(fast_days)
    if len(labeled) < MIN_SAMPLES:
        log.warning(kv(
            event="insufficient_data", samples=len(labeled), min_samples=MIN_SAMPLES,
        ))
        return {
            "status": "insufficient_data",
            "samples": len(labeled),
            "weights": DEFAULT_SCORE_WEIGHTS,
        }

    base_rate = sum(1 for _, fast in labeled if fast) / len(labeled)
    if base_rate == 0:
        return {"status": "no_fast_labels", "weights": DEFAULT_SCORE_WEIGHTS}

    new_weights: dict[str, int] = {}
    lifts: dict[str, float | None] = {}
    n_fired: dict[str, int] = {}

    for signal, default_w in DEFAULT_SCORE_WEIGHTS.items():
        lift, n = _lift_for(labeled, signal, base_rate)
        lifts[signal] = round(lift, 3) if lift is not None else None
        n_fired[signal] = n
        if lift is None:
            new_weights[signal] = default_w  # mantém default — sem dados
            continue
        adjusted = int(round(default_w * lift))
        new_weights[signal] = max(MIN_WEIGHT, min(MAX_WEIGHT, adjusted))

    if not dry_run:
        CONFIG_DIR.mkdir(exist_ok=True)
        WEIGHTS_PATH.write_text(
            json.dumps(new_weights, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    log.info(kv(
        event="weights_optimized",
        samples=len(labeled),
        base_rate=round(base_rate, 3),
        lifts=str(lifts),
    ))

    return {
        "status": "ok",
        "samples": len(labeled),
        "base_rate": round(base_rate, 3),
        "lifts": lifts,
        "n_fired": n_fired,
        "old_weights": DEFAULT_SCORE_WEIGHTS,
        "new_weights": new_weights,
        "path": str(WEIGHTS_PATH),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", type=int, default=FAST_THRESHOLD_DAYS)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    result = optimize(fast_days=args.days, dry_run=args.dry_run)
    print(f"status:       {result['status']}")
    print(f"samples:      {result.get('samples')}")
    if result["status"] == "ok":
        print(f"base_rate:    {result['base_rate']}")
        print()
        print(f"{'signal':22s}  n_fired  lift    old → new")
        print("-" * 55)
        for sig, new_w in result["new_weights"].items():
            old_w = result["old_weights"][sig]
            lift = result["lifts"].get(sig)
            lift_str = f"{lift:5.2f}" if lift is not None else "  -  "
            print(f"{sig:22s}  {result['n_fired'][sig]:6d}  {lift_str}  {old_w:3d} → {new_w:3d}")
        if not args.dry_run:
            print(f"\nsaved to {result['path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
