"""
Re-ordenação de alertas pendentes por prioridade antes do envio.

Em volumes altos, o alert_engine v8 envia eventos `watcher_match` na ordem
em que aparecem no banco. Para um usuário pagante, o melhor deal pode chegar
DEPOIS de 50 deals medianos. v10 inverte isso: ranqueia tudo, envia os
melhores primeiro.

priority_score por (listing, watcher):

    base =
        + opportunity_probability * 50          (0..50)
        + min(30, discount_percentage * 0.5)    (0..30)
        + min(20, fresh_opportunity_score*0.2)  (0..20)
        + min(10, liquidity_score / 10)         (0..10)

    plan_boost (multiplicador):
        premium → 1.5
        pro     → 1.2
        free    → 1.0
        null    → 1.0

    score = base * plan_boost

A função `process_with_priority()` substitui `process_pending_watcher_matches`:
ela busca os mesmos eventos pendentes, ordena por score desc, e chama
`alert_engine.send_for_match` em ordem. Dedup continua via `alert_sent` no
alert_engine — alert_priority não duplica essa lógica.

Uso:
    python alert_priority_engine.py
    python alert_priority_engine.py --dry-run
    python alert_priority_engine.py --top 50
"""

from __future__ import annotations

import argparse

from db import connect, init_db
from logging_setup import get_logger, kv

log = get_logger("alert_priority")

PLAN_BOOST: dict[str | None, float] = {
    "premium": 1.5,
    "pro": 1.2,
    "free": 1.0,
    None: 1.0,
}


def compute_priority_score(listing_row, watcher_row) -> float:
    base = 0.0

    prob = listing_row["opportunity_probability"] if "opportunity_probability" in listing_row.keys() else 0
    if prob:
        base += prob * 50

    discount = listing_row["discount_percentage"] or 0
    if discount > 0:
        base += min(30, discount * 0.5)

    fresh = listing_row["fresh_opportunity_score"] or 0
    if fresh:
        base += min(20, fresh * 0.2)

    liquidity = listing_row["liquidity_score"] or 0
    if liquidity:
        base += min(10, liquidity / 10)

    plan = watcher_row["plan"] if watcher_row and "plan" in watcher_row.keys() else None
    boost = PLAN_BOOST.get(plan, 1.0)
    return round(base * boost, 2)


def _load_pending_with_context(limit: int = 200) -> list[dict]:
    """Carrega eventos `watcher_match` recentes + listing + watcher num único join.
    Filtra os que já têm `alert_sent` para qualquer canal — economiza queries."""
    init_db()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT
                e.listing_id, e.new_value AS watch_field, e.at AS match_at,
                l.opportunity_probability, l.discount_percentage,
                l.fresh_opportunity_score, l.liquidity_score,
                l.current_title, l.current_price, l.url, l.current_currency,
                l.current_location
              FROM events e
              JOIN listings l ON l.id = e.listing_id
             WHERE e.event_type = 'watcher_match'
             ORDER BY e.at DESC
             LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def _parse_watch_id(field: str | None) -> int | None:
    if not field or not field.startswith("watch_id="):
        return None
    try:
        return int(field.split("=", 1)[1])
    except ValueError:
        return None


def _load_watcher(conn, watch_id: int) -> dict | None:
    row = conn.execute(
        "SELECT watch_id, plan, keyword, region FROM watchers WHERE watch_id = ?",
        (watch_id,),
    ).fetchone()
    return dict(row) if row else None


def rank_pending(limit: int = 200) -> list[dict]:
    """Retorna eventos pendentes ordenados por priority_score desc."""
    init_db()
    rows = _load_pending_with_context(limit)
    if not rows:
        return []

    # Carrega watchers num batch
    watcher_ids = set()
    for r in rows:
        wid = _parse_watch_id(r.get("watch_field"))
        if wid is not None:
            watcher_ids.add(wid)

    watchers_by_id: dict[int, dict] = {}
    if watcher_ids:
        with connect() as conn:
            placeholders = ",".join("?" * len(watcher_ids))
            for r in conn.execute(
                f"SELECT watch_id, plan, keyword, region FROM watchers "
                f"WHERE watch_id IN ({placeholders})",
                tuple(watcher_ids),
            ).fetchall():
                watchers_by_id[r["watch_id"]] = dict(r)

    enriched = []
    for r in rows:
        wid = _parse_watch_id(r.get("watch_field"))
        watcher = watchers_by_id.get(wid) if wid else None
        score = compute_priority_score(r, watcher or {})
        enriched.append({**r, "watch_id": wid, "priority_score": score})

    enriched.sort(key=lambda x: -x["priority_score"])
    return enriched


def process_with_priority(dry_run: bool = False, top: int = 200) -> dict:
    """Substituição do alert_engine.process_pending_watcher_matches:
    ranqueia primeiro, envia em ordem de prioridade. Reusa send_for_match."""
    from alert_engine import send_for_match

    ranked = rank_pending(limit=top)
    stats = {
        "ranked": len(ranked),
        "sent_telegram": 0,
        "sent_discord": 0,
        "skipped_dedup": 0,
        "errors": 0,
    }

    for entry in ranked:
        wid = entry.get("watch_id")
        if wid is None:
            stats["errors"] += 1
            continue
        result = send_for_match(entry["listing_id"], wid, dry_run=dry_run)
        if result.get("telegram") is True:
            stats["sent_telegram"] += 1
        if result.get("discord") is True:
            stats["sent_discord"] += 1
        if result.get("telegram") == "dedup" or result.get("discord") == "dedup":
            stats["skipped_dedup"] += 1

    log.info(kv(event="priority_alerts_processed", **stats))
    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--top", type=int, default=200)
    ap.add_argument("--rank-only", action="store_true",
                    help="só imprime ranking sem enviar")
    args = ap.parse_args()

    if args.rank_only:
        ranked = rank_pending(limit=args.top)
        for r in ranked[:30]:
            print(f"  [{r['priority_score']:>7.2f}] {r['listing_id']}  "
                  f"{(r.get('current_title') or '')[:60]}")
        print(f"\ntotal ranked: {len(ranked)}")
        return 0

    result = process_with_priority(dry_run=args.dry_run, top=args.top)
    for k, v in result.items():
        print(f"  {k:18s} {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
