"""
Scheduler para watchers.

A v9 introduziu intervalos por priority (1=10min, 2=30min, 3=1h). v10 vai
além: dentro do conjunto de watchers DUE, ordena por dynamic_priority — um
score combinando popularidade, plano e histórico.

Quando temos 50 watchers due e concurrency=3, importa quem roda primeiro:
o usuário pagante quer ver "iphone Araçatuba" rodar antes do hobbyist
monitorando "abajur retrô".

dynamic_priority(watcher):
    base = 100 - priority * 20         # P1=80, P2=60, P3=40
    + min(20, num_users * 5)            # popularidade do (kw, region)
    + min(20, match_count * 0.5)        # histórico
    + plan_bonus                        # premium=30, pro=15

Funções:
    schedule_due(min_interval=DEFAULT_INTERVAL_SECONDS) -> list[int]
        Devolve watch_ids ordenados (mais prioritário primeiro), filtrando
        apenas os DUE segundo seus intervalos individuais.

    debug() -> tabela com scores

Uso:
    python watcher_scheduler.py
    python watcher_scheduler.py --json
"""

from __future__ import annotations

import argparse
import json

from db import connect, init_db
from logging_setup import get_logger
from watcher_engine import DEFAULT_INTERVAL_SECONDS, _select_due_ids

log = get_logger("watcher_scheduler")

PLAN_BONUS = {"premium": 30, "pro": 15, "free": 0, None: 0}


def compute_dynamic_priority(
    watcher: dict, num_users: int = 1, match_count: int = 0,
) -> float:
    pri = watcher.get("priority") or 2
    base = 100 - (pri * 20)  # P1=80 P2=60 P3=40

    base += min(20, num_users * 5)
    base += min(20, match_count * 0.5)

    plan = watcher.get("plan")
    base += PLAN_BONUS.get(plan, 0)

    return float(base)


def _load_watchers_with_context() -> list[dict]:
    """Faz join à mão entre watchers + (counts de popularidade e match history)."""
    init_db()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT w.watch_id, w.keyword, w.region, w.priority, w.plan,
                   w.last_run_at, w.is_active,
                   (SELECT COUNT(*) FROM watcher_results wr
                     WHERE wr.watch_id = w.watch_id) AS match_count
              FROM watchers w
             WHERE w.is_active = 1
            """
        ).fetchall()
        rows = [dict(r) for r in rows]

    # Popularidade: GROUP BY em Python (Unicode-safe, mesmo motivo do v9)
    pop: dict[tuple[str, str], int] = {}
    for r in rows:
        key = (
            (r["keyword"] or "").lower(),
            (r["region"] or "").lower(),
        )
        pop[key] = pop.get(key, 0) + 1

    for r in rows:
        key = (
            (r["keyword"] or "").lower(),
            (r["region"] or "").lower(),
        )
        r["num_users"] = pop[key]
    return rows


def schedule_due(
    min_interval_seconds: int = DEFAULT_INTERVAL_SECONDS,
) -> list[int]:
    """Retorna watch_ids ordenados por dynamic_priority desc, restritos
    aos DUE segundo seus intervalos individuais."""
    rows = _load_watchers_with_context()
    if not rows:
        return []

    due_ids = set(_select_due_ids(rows, min_interval_seconds))
    if not due_ids:
        return []

    scored: list[tuple[int, float]] = []
    for r in rows:
        if r["watch_id"] not in due_ids:
            continue
        score = compute_dynamic_priority(
            r, num_users=r["num_users"], match_count=r["match_count"],
        )
        scored.append((r["watch_id"], score))

    scored.sort(key=lambda x: -x[1])
    return [wid for wid, _ in scored]


def debug() -> list[dict]:
    """Devolve todos os watchers ativos com seus scores, due ou não."""
    rows = _load_watchers_with_context()
    out = []
    for r in rows:
        score = compute_dynamic_priority(
            r, num_users=r["num_users"], match_count=r["match_count"],
        )
        out.append({
            "watch_id": r["watch_id"],
            "keyword": r["keyword"],
            "region": r["region"],
            "priority": r["priority"],
            "plan": r["plan"],
            "num_users": r["num_users"],
            "match_count": r["match_count"],
            "dynamic_score": score,
        })
    out.sort(key=lambda x: -x["dynamic_score"])
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    rows = debug()
    if args.json:
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return 0

    print(f"{'#':>4s}  {'kw':<20s} {'region':<15s} {'P':>2s} {'plan':>8s} "
          f"{'users':>5s} {'matches':>7s}  {'score':>6s}")
    print("-" * 85)
    for r in rows:
        print(f"{r['watch_id']:>4d}  "
              f"{(r['keyword'] or '')[:20]:<20s} "
              f"{(r['region'] or '-')[:15]:<15s} "
              f"{r['priority']:>2d} "
              f"{(r['plan'] or '-'):>8s} "
              f"{r['num_users']:>5d} "
              f"{r['match_count']:>7d}  "
              f"{r['dynamic_score']:>6.1f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
