"""
Heurísticas para flagar anúncios que *parecem* oportunidades.

Regras atuais:
  1. urgency_keyword   — título contém palavras de urgência
  2. below_market      — preço < média − 1.5σ dos anúncios com palavra-chave comum no título
  3. price_drop        — histórico de preço mostra queda >= 15% em qualquer delta
  4. short_description — descrição ausente ou com menos de 30 caracteres

Flags são gravadas como eventos `event_type='opportunity_flag'` com
`new_value = "<rule>: <reason>"`. A função dedup checa se a mesma regra
já foi flagada para aquele listing — evita duplicar a cada passada do
monitor.

Uso (CLI):
    python opportunities.py          # scan completo + imprime sumário
    python opportunities.py --dry-run
"""

from __future__ import annotations

import argparse
import re
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from analytics import _to_float
from db import (
    all_active_listings, connect, insert_event, latest_snapshot_payload,
    now_iso, price_history_for,
)
from logging_setup import get_logger, kv

log = get_logger("opportunities")

URGENT_PATTERNS = [
    "urgente", "preciso vender", "hoje", "aceito troca",
    "abaixo do mercado", "queima", "desapego", "promoção",
    "só hoje", "passo rápido", "baixei",
]

PRICE_DROP_PCT = 0.15
BELOW_MARKET_SIGMAS = 1.5
SHORT_DESC_CHARS = 30
MIN_GROUP_SIZE = 4  # só aplicamos below_market se houver comparáveis suficientes

KEYWORD_RE = re.compile(r"[a-zà-ÿ0-9]+", re.IGNORECASE)


@dataclass
class Flag:
    rule: str
    reason: str

    def serialize(self) -> str:
        return f"{self.rule}: {self.reason}"


# --- extração de tokens ---------------------------------------------------

def _tokens(title: str) -> set[str]:
    toks = {t.lower() for t in KEYWORD_RE.findall(title) if len(t) >= 3}
    return toks


# --- regras ---------------------------------------------------------------

def check_urgency(title: str) -> Flag | None:
    t = title.lower()
    for pat in URGENT_PATTERNS:
        if pat in t:
            return Flag("urgency_keyword", f"title contains '{pat}'")
    return None


def check_short_description(desc: str | None) -> Flag | None:
    if desc is None or len(desc.strip()) < SHORT_DESC_CHARS:
        return Flag("short_description",
                    f"description length {len(desc or '')} < {SHORT_DESC_CHARS}")
    return None


def check_price_drop(history: list[float]) -> Flag | None:
    if len(history) < 2:
        return None
    first, last = history[0], history[-1]
    if first <= 0:
        return None
    drop = (first - last) / first
    if drop >= PRICE_DROP_PCT:
        return Flag("price_drop",
                    f"price dropped {drop * 100:.1f}% from {first:.2f} to {last:.2f}")
    return None


def check_below_market(
    price: float, title_tokens: set[str], group_stats: dict
) -> Flag | None:
    """group_stats: dict[token -> (mean, stdev, n)]. Usa o token com maior n que
    tenha tamanho suficiente para ser comparativo."""
    best = None
    for tok in title_tokens:
        s = group_stats.get(tok)
        if not s:
            continue
        mean, stdev, n = s
        if n < MIN_GROUP_SIZE or stdev <= 0:
            continue
        if best is None or n > best[2]:
            best = (mean, stdev, n, tok)
    if best is None:
        return None
    mean, stdev, n, tok = best
    threshold = mean - BELOW_MARKET_SIGMAS * stdev
    if price < threshold:
        return Flag(
            "below_market",
            f"price {price:.0f} < threshold {threshold:.0f} "
            f"(mean={mean:.0f} σ={stdev:.0f} n={n} keyword='{tok}')"
        )
    return None


# --- orquestrador ---------------------------------------------------------

def _build_group_stats(listings: list[dict]) -> dict:
    """Para cada token comum no universo, calcula (mean, stdev, n) de preço."""
    token_prices: dict[str, list[float]] = {}
    for row in listings:
        price = _to_float(row["current_price"])
        if price is None:
            continue
        title = row["current_title"] or ""
        for tok in _tokens(title):
            token_prices.setdefault(tok, []).append(price)

    stats: dict[str, tuple[float, float, int]] = {}
    for tok, prices in token_prices.items():
        if len(prices) < MIN_GROUP_SIZE:
            continue
        mean = statistics.fmean(prices)
        stdev = statistics.pstdev(prices) if len(prices) > 1 else 0.0
        stats[tok] = (mean, stdev, len(prices))
    return stats


def _already_flagged(conn, listing_id: str, rule: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM events WHERE listing_id = ? AND event_type = 'opportunity_flag' "
        "AND new_value LIKE ? LIMIT 1",
        (listing_id, f"{rule}:%"),
    ).fetchone()
    return row is not None


def scan(dry_run: bool = False) -> dict:
    with connect() as conn:
        listings = conn.execute(
            "SELECT * FROM listings WHERE is_removed = 0 AND current_title IS NOT NULL"
        ).fetchall()
        listings = [dict(r) for r in listings]
        group_stats = _build_group_stats(listings)

        total_flags = 0
        per_rule: dict[str, int] = {}
        now = now_iso()

        for row in listings:
            lid = row["id"]
            title = row["current_title"] or ""
            price = _to_float(row["current_price"])

            payload = latest_snapshot_payload(conn, lid) or {}
            description = payload.get("description") if isinstance(payload, dict) else None

            ph = [r["price"] for r in price_history_for(conn, lid)]

            flags: list[Flag] = []
            f = check_urgency(title)
            if f: flags.append(f)
            f = check_short_description(description)
            if f: flags.append(f)
            f = check_price_drop(ph)
            if f: flags.append(f)
            if price is not None:
                f = check_below_market(price, _tokens(title), group_stats)
                if f: flags.append(f)

            for fl in flags:
                if _already_flagged(conn, lid, fl.rule):
                    continue
                if not dry_run:
                    insert_event(conn, lid, now, "opportunity_flag", None, fl.serialize())
                per_rule[fl.rule] = per_rule.get(fl.rule, 0) + 1
                total_flags += 1
                log.info(kv(listing=lid, rule=fl.rule, flagged=True))

        return {
            "total_flagged": total_flags,
            "per_rule": per_rule,
            "listings_scanned": len(listings),
            "keywords_with_stats": len(group_stats),
        }


# --- opportunity_score (0..100) --------------------------------------------

# Pesos padrão — sobrescritíveis via config/score_weights.json (gerado por
# score_optimizer.py). SCORE_WEIGHTS é lido em compute_score e pode ser
# recarregado a qualquer momento via reload_weights().

import json as _json
from pathlib import Path as _Path

DEFAULT_SCORE_WEIGHTS = {
    "discount_big":    40,   # desconto > 30%
    "discount_mid":    20,   # desconto > 15%
    "urgency":         15,
    "short_desc":      10,
    "recent":          20,   # first_seen < 2h
    "below_percentile": 15,  # preço abaixo do p25 (proxy: discount > 25%)
}

_WEIGHTS_PATH = _Path("config/score_weights.json")


def reload_weights() -> dict[str, int]:
    """Lê config/score_weights.json (se existir), mescla com defaults, e
    atualiza SCORE_WEIGHTS globalmente. Retorna o dict resultante."""
    global SCORE_WEIGHTS
    merged = dict(DEFAULT_SCORE_WEIGHTS)
    if _WEIGHTS_PATH.exists():
        try:
            data = _json.loads(_WEIGHTS_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                for k, v in data.items():
                    if k in merged and isinstance(v, (int, float)):
                        merged[k] = int(v)
        except (_json.JSONDecodeError, OSError):
            log.warning(kv(event="weights_load_failed", path=str(_WEIGHTS_PATH)))
    SCORE_WEIGHTS = merged
    return merged


SCORE_WEIGHTS = reload_weights()


def compute_score(listing_row, payload: dict | None) -> tuple[int, list[str]]:
    """Pura: recebe a row do listing + payload do último snapshot, devolve
    (score 0..100, lista de reasons). Não toca DB."""
    score = 0
    reasons: list[str] = []

    discount = listing_row["discount_percentage"]
    if discount is not None:
        if discount > 30:
            score += SCORE_WEIGHTS["discount_big"]
            reasons.append(f"discount>{30}% ({discount:.0f}%)")
        elif discount > 15:
            score += SCORE_WEIGHTS["discount_mid"]
            reasons.append(f"discount>{15}% ({discount:.0f}%)")
        if discount > 25:
            score += SCORE_WEIGHTS["below_percentile"]
            reasons.append("below_p25_proxy")

    title = (listing_row["current_title"] or "").lower()
    if any(p in title for p in URGENT_PATTERNS):
        score += SCORE_WEIGHTS["urgency"]
        reasons.append("urgency_keyword")

    desc = None
    if payload and isinstance(payload, dict):
        desc = payload.get("description")
    if not desc or len(str(desc).strip()) < SHORT_DESC_CHARS:
        score += SCORE_WEIGHTS["short_desc"]
        reasons.append("short_description")

    try:
        fs = datetime.fromisoformat(listing_row["first_seen_at"])
        if fs.tzinfo is None:
            fs = fs.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) - fs < timedelta(hours=2):
            score += SCORE_WEIGHTS["recent"]
            reasons.append("recent<2h")
    except (TypeError, ValueError):
        pass

    return min(score, 100), reasons


def score_all_listings() -> int:
    """Recomputa opportunity_score para todos os listings ativos. Retorna N."""
    updated = 0
    with connect() as conn:
        listings = all_active_listings(conn)
        for l in listings:
            payload = latest_snapshot_payload(conn, l["id"])
            score, _ = compute_score(l, payload)
            conn.execute(
                "UPDATE listings SET opportunity_score = ? WHERE id = ?",
                (score, l["id"]),
            )
            updated += 1
    log.info(kv(event="scores_updated", count=updated))
    return updated


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true",
                    help="não grava eventos, só conta o que flagaria")
    args = ap.parse_args()

    result = scan(dry_run=args.dry_run)
    print(f"listings_scanned:     {result['listings_scanned']}")
    print(f"keywords_with_stats:  {result['keywords_with_stats']}")
    print(f"total_flagged:        {result['total_flagged']}")
    for rule, n in sorted(result["per_rule"].items(), key=lambda x: -x[1]):
        print(f"  {rule:22s} {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
