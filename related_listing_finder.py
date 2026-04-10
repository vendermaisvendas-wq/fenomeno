"""
Geração de queries derivadas a partir de um anúncio descoberto.

Quando o discovery encontra "iPhone 12 128GB preto bateria 90%", queremos
extrair queries relacionadas que possam encontrar listings parecidos no DDG:

    iphone 12
    iphone 128gb
    iphone

Essas queries voltam para o `marketplace_deep_discovery` e alimentam o
`discovery_graph` — cada listing pode gerar nova rodada de descoberta.

A função é PURA: recebe título, devolve lista. Não toca DB.

Estratégias (em ordem de prioridade):
    1. brand + model conhecido (vehicle_model.KNOWN_MODELS)
    2. brand sozinho
    3. modelo sozinho
    4. brand + token significativo do título (excluindo ano/marca/modelo)
    5. par de tokens significativos (≥4 chars, não numéricos)

Filtros:
    - Queries muito curtas (<3 chars total)
    - Queries só com stopwords
    - Queries idênticas (dedup case-insensitive)
    - Limite máximo via max_queries

Uso:
    python related_listing_finder.py "iPhone 12 128GB preto bateria 90%"
    python related_listing_finder.py "Toyota Hilux SRV 2020 Diesel 4x4"
"""

from __future__ import annotations

import argparse

from title_normalizer import extract_brand, tokens
from vehicle_model import KNOWN_MODELS

DEFAULT_MAX_QUERIES = 5
MIN_QUERY_LEN = 3
MIN_TOKEN_LEN = 3


def _is_year_like(token: str) -> bool:
    return token.isdigit() and len(token) == 4


def _significant_tokens(toks: set[str], exclude: set[str]) -> list[str]:
    """Tokens >=3 chars, não-numéricos, não em exclude. Determinístico."""
    return sorted(
        t for t in toks
        if len(t) >= MIN_TOKEN_LEN
        and not _is_year_like(t)
        and t not in exclude
    )


def derive_queries(
    title: str | None,
    max_queries: int = DEFAULT_MAX_QUERIES,
) -> list[str]:
    if not title or not title.strip():
        return []

    out: list[str] = []
    seen: set[str] = set()

    def _add(q: str) -> bool:
        q = q.strip().lower()
        if not q or len(q) < MIN_QUERY_LEN or q in seen:
            return False
        seen.add(q)
        out.append(q)
        return len(out) >= max_queries

    toks = tokens(title)
    brand = extract_brand(title)
    model = next(iter(sorted(toks & KNOWN_MODELS)), None)

    # 1. brand + model (mais preciso)
    if brand and model:
        if _add(f"{brand} {model}"):
            return out

    # 2. só model
    if model:
        if _add(model):
            return out

    # 3. só brand
    if brand:
        if _add(brand):
            return out

    # Tokens significativos: exclui brand, model, anos, model_tokens
    exclude = set()
    if brand:
        exclude.add(brand)
    if model:
        exclude.add(model)
    significant = _significant_tokens(toks, exclude)

    # 4. brand + primeiro significativo
    if brand and significant:
        if _add(f"{brand} {significant[0]}"):
            return out

    # 5. par de tokens significativos
    if len(significant) >= 2:
        if _add(f"{significant[0]} {significant[1]}"):
            return out

    # 6. terceiro singular como fallback (se ainda houver espaço)
    for tok in significant:
        if _add(tok):
            return out

    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("title")
    ap.add_argument("--max", type=int, default=DEFAULT_MAX_QUERIES)
    args = ap.parse_args()

    qs = derive_queries(args.title, max_queries=args.max)
    for q in qs:
        print(q)
    return 0 if qs else 1


if __name__ == "__main__":
    raise SystemExit(main())
