"""
Cobertura geográfica: extração de cidade/estado dos anúncios do Marketplace
e cálculo de `coverage_score` por cidade.

O FB entrega `location_text` em formatos variados:
    "São Paulo, SP"
    "São Paulo, SP, Brasil"
    "Rio de Janeiro - RJ"
    "Campinas, São Paulo"
    "Brasília"                 (só cidade)
    "DF"                       (só estado)

O parser é best-effort:
  parse_location(text) -> (city|None, state|None)

O state é sempre normalizado para sigla de 2 letras (SP, RJ, ...).
O city é canonicalizado (title case + strip accents opcional).

Funções principais:
    parse_location(text)             pura
    apply_to_listings()              popula listings.city / listings.state
    compute_coverage()               stats por cidade + coverage_score
    persist_coverage()               grava em `geo_coverage`

`coverage_score` (0..100) é uma métrica do quão bem cobrimos aquela cidade:
  - log(active_count)  peso 50    → volume de dados
  - log(distinct tokens) peso 30  → diversidade de categorias
  - recência (dias desde último fetch) peso 20 → freshness

Uso:
    python geo_coverage.py                   # tudo (apply + compute + persist)
    python geo_coverage.py --apply-only      # só popula city/state nos listings
    python geo_coverage.py --compute-only    # só recalcula stats (usa city já existente)
"""

from __future__ import annotations

import argparse
import math
import statistics
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from db import connect, init_db, now_iso
from logging_setup import get_logger, kv
from price_normalizer import parse as parse_price
from title_normalizer import tokens

log = get_logger("geo_coverage")

# Siglas oficiais dos 26 estados + DF
STATE_ABBR = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
    "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
    "RS", "RO", "RR", "SC", "SP", "SE", "TO",
}

# Mapa nome completo → sigla (para "São Paulo, São Paulo" → SP)
STATE_NAME_TO_ABBR = {
    "acre": "AC", "alagoas": "AL", "amapa": "AP", "amazonas": "AM",
    "bahia": "BA", "ceara": "CE", "distrito federal": "DF",
    "espirito santo": "ES", "goias": "GO", "maranhao": "MA",
    "mato grosso": "MT", "mato grosso do sul": "MS",
    "minas gerais": "MG", "para": "PA", "paraiba": "PB", "parana": "PR",
    "pernambuco": "PE", "piaui": "PI", "rio de janeiro": "RJ",
    "rio grande do norte": "RN", "rio grande do sul": "RS",
    "rondonia": "RO", "roraima": "RR", "santa catarina": "SC",
    "sao paulo": "SP", "sergipe": "SE", "tocantins": "TO",
}

COUNTRY_TOKENS = {"brasil", "brazil", "br"}


def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


def _canonical_state(token: str | None, allow_full_name: bool = True) -> str | None:
    """Sigla 2-letras é sempre state. Full name só quando `allow_full_name` —
    usado pelo parser para evitar a ambiguidade "São Paulo" (cidade) vs.
    "São Paulo" (estado) no primeiro elemento da lista."""
    if not token:
        return None
    t = token.strip()
    if len(t) == 2 and t.upper() in STATE_ABBR:
        return t.upper()
    if allow_full_name:
        normalized = _strip_accents(t.lower())
        return STATE_NAME_TO_ABBR.get(normalized)
    return None


def _canonical_city(name: str) -> str:
    """Title case preservando acentos, strip edges."""
    return name.strip().title()


def parse_location(text: str | None) -> tuple[str | None, str | None]:
    """Melhor esforço: retorna (city, state). state é sigla ou None."""
    if not text:
        return None, None

    raw = text.strip()
    # Separador primário é vírgula; alguns usam " - "
    if "," in raw:
        parts = [p.strip() for p in raw.split(",") if p.strip()]
    elif " - " in raw:
        parts = [p.strip() for p in raw.split(" - ") if p.strip()]
    else:
        parts = [raw]

    # Remove sufixos "Brasil", "BR"
    parts = [
        p for p in parts
        if _strip_accents(p.lower()) not in COUNTRY_TOKENS
    ]
    if not parts:
        return None, None

    # Identifica estado: siglas 2-letras sempre; full names apenas quando
    # NÃO for o primeiro elemento (evita "São Paulo, SP" → (SP, SP))
    state: str | None = None
    city_parts: list[str] = []
    for i, p in enumerate(parts):
        st = _canonical_state(p, allow_full_name=(i > 0))
        if st and state is None:
            state = st
        else:
            city_parts.append(p)

    if city_parts:
        return _canonical_city(city_parts[0]), state
    # Só sigla (ex.: "RJ") — sem city parseável
    return None, state


# --- aplicação em batch -------------------------------------------------

def apply_to_listings() -> dict:
    """Percorre `listings.current_location` e popula city/state."""
    init_db()
    updates: list[tuple[str | None, str | None, str]] = []
    parsed_count = 0

    with connect() as conn:
        rows = conn.execute(
            "SELECT id, current_location FROM listings "
            "WHERE current_location IS NOT NULL"
        ).fetchall()

        for r in rows:
            city, state = parse_location(r["current_location"])
            if city or state:
                parsed_count += 1
            updates.append((city, state, r["id"]))

        if updates:
            conn.executemany(
                "UPDATE listings SET city = ?, state = ? WHERE id = ?",
                updates,
            )

    result = {"processed": len(updates), "parsed": parsed_count}
    log.info(kv(event="geo_applied", **result))
    return result


# --- stats por cidade ---------------------------------------------------

@dataclass
class CityStats:
    city: str
    state: str | None
    total: int
    active: int
    avg_price: float | None
    avg_discount: float | None
    distinct_tokens: int
    last_seen_at: str | None
    coverage_score: int


def _compute_coverage_score(
    active: int, distinct_tokens: int, days_since_last: float | None,
) -> int:
    # Componente 1: volume (log-escala, saturação em ~1000 ativos)
    vol = min(50, int(50 * math.log1p(active) / math.log1p(1000)))
    # Componente 2: diversidade (log-escala, saturação em ~200 tokens)
    div = min(30, int(30 * math.log1p(distinct_tokens) / math.log1p(200)))
    # Componente 3: freshness (decai linear até 30 dias, depois zero)
    if days_since_last is None:
        fresh = 0
    else:
        fresh = max(0, int(20 * (1 - days_since_last / 30)))
    return min(100, vol + div + fresh)


def compute_coverage() -> list[CityStats]:
    init_db()
    now = datetime.now(timezone.utc)

    with connect() as conn:
        rows = conn.execute(
            """
            SELECT city, state, is_removed, current_title, current_price,
                   discount_percentage, last_seen_at
              FROM listings
             WHERE city IS NOT NULL
            """
        ).fetchall()

    buckets: dict[tuple[str, str | None], list[dict]] = {}
    for r in rows:
        key = (r["city"], r["state"])
        buckets.setdefault(key, []).append(dict(r))

    results: list[CityStats] = []
    for (city, state), items in buckets.items():
        active = sum(1 for it in items if not it["is_removed"])
        total = len(items)

        prices = [parse_price(it["current_price"]) for it in items]
        prices = [p for p in prices if p is not None]
        avg_price = round(statistics.fmean(prices), 2) if prices else None

        discounts = [
            it["discount_percentage"] for it in items
            if it["discount_percentage"] is not None
        ]
        avg_discount = round(statistics.fmean(discounts), 2) if discounts else None

        all_tokens: set[str] = set()
        for it in items:
            all_tokens |= tokens(it["current_title"])
        distinct_tokens = len(all_tokens)

        last_seen_iso = max((it["last_seen_at"] for it in items if it["last_seen_at"]), default=None)
        days_since = None
        if last_seen_iso:
            try:
                dt = datetime.fromisoformat(last_seen_iso)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                days_since = (now - dt).total_seconds() / 86400.0
            except ValueError:
                days_since = None

        score = _compute_coverage_score(active, distinct_tokens, days_since)

        results.append(CityStats(
            city=city, state=state, total=total, active=active,
            avg_price=avg_price, avg_discount=avg_discount,
            distinct_tokens=distinct_tokens,
            last_seen_at=last_seen_iso,
            coverage_score=score,
        ))

    results.sort(key=lambda s: -s.coverage_score)
    return results


def persist_coverage(stats: Iterable[CityStats]) -> int:
    init_db()
    computed_at = now_iso()
    count = 0
    with connect() as conn:
        # Reseta e regrava (semantic: snapshot atual)
        conn.execute("DELETE FROM geo_coverage")
        for s in stats:
            conn.execute(
                """
                INSERT INTO geo_coverage
                  (city, state, listings_count, active_count, avg_price,
                   avg_discount, coverage_score, computed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (s.city, s.state, s.total, s.active,
                 s.avg_price, s.avg_discount, s.coverage_score, computed_at),
            )
            count += 1
    log.info(kv(event="coverage_persisted", cities=count))
    return count


def run(apply: bool = True, compute: bool = True, persist: bool = True) -> dict:
    result = {}
    if apply:
        result.update(apply_to_listings())
    if compute:
        stats = compute_coverage()
        result["cities"] = len(stats)
        if persist:
            persist_coverage(stats)
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply-only", action="store_true")
    ap.add_argument("--compute-only", action="store_true")
    args = ap.parse_args()

    if args.apply_only:
        result = apply_to_listings()
    elif args.compute_only:
        stats = compute_coverage()
        persist_coverage(stats)
        result = {"cities": len(stats)}
    else:
        result = run()

    for k, v in result.items():
        print(f"  {k:20s} {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
