"""
Geração automática de variações de keyword para discovery.

Estratégia: pure rule-based, vocabulário curado. Não usa modelo — para o
universo do Marketplace BR, listas curtas funcionam tão bem quanto e são
explicáveis. As listas são extensíveis sem mexer em código.

Categorias de variação (em ordem de prioridade):
    1. Versões / modelos conhecidos     (iphone → iphone 11, 12, 13...)
    2. Marca explícita                   (iphone → iphone apple)
    3. Sinônimos comuns                  (notebook → laptop)
    4. Modificadores genéricos           (X → "X usado", "X seminovo", "X barato")

Funções:
    expand(keyword, max_variations=8) -> list[str]
    expand_with_context(keyword, region, max_variations) -> list[str]

Idempotente, determinístico, puro.

Uso:
    python keyword_expander.py iphone
    python keyword_expander.py "playstation 5" --max 12
"""

from __future__ import annotations

import argparse


# Modelos / versões conhecidas. Trigger é match exato (sem substring) para
# evitar que "iphone case" gere "iphone 11", "iphone 12", etc.
VERSION_EXPANSIONS: dict[str, list[str]] = {
    "iphone": [
        "iphone 11", "iphone 12", "iphone 13", "iphone 14", "iphone 15",
        "iphone xr", "iphone xs", "iphone se",
    ],
    "ipad": ["ipad pro", "ipad air", "ipad mini"],
    "macbook": ["macbook pro", "macbook air"],
    "playstation": ["playstation 4", "playstation 5"],
    "ps": ["ps4", "ps5"],
    "xbox": ["xbox one", "xbox series s", "xbox series x"],
    "galaxy": ["galaxy s21", "galaxy s22", "galaxy s23", "galaxy a"],
    "redmi": ["redmi note 10", "redmi note 11", "redmi note 12"],
    "moto g": ["moto g32", "moto g52", "moto g54"],
    "hilux": ["hilux srv", "hilux sr", "hilux srx"],
    "civic": ["civic exl", "civic lxr", "civic touring"],
    "corolla": ["corolla xei", "corolla altis", "corolla gli"],
    "onix": ["onix lt", "onix lt2", "onix premier"],
    "gol": ["gol g4", "gol g5", "gol g6", "gol trend"],
    "cg": ["cg 150", "cg 160", "cg titan", "cg fan", "cg start"],
    "factor": ["factor 125", "factor 150"],
}

# Marca explícita — adiciona contexto que o usuário pode ter omitido
BRAND_EXPANSIONS: dict[str, list[str]] = {
    "iphone": ["iphone apple"],
    "ipad": ["ipad apple"],
    "macbook": ["macbook apple"],
    "airpods": ["airpods apple"],
    "galaxy": ["samsung galaxy"],
    "moto g": ["motorola moto g"],
    "moto e": ["motorola moto e"],
    "redmi": ["xiaomi redmi"],
    "ps4": ["playstation 4"],
    "ps5": ["playstation 5"],
}

# Sinônimos diretos — bidirecional
SYNONYMS: dict[str, list[str]] = {
    "notebook": ["laptop"],
    "laptop": ["notebook"],
    "celular": ["smartphone"],
    "smartphone": ["celular"],
    "moto": ["motocicleta"],
    "motocicleta": ["moto"],
}

# Modificadores genéricos — anexados ao final
COMMON_MODIFIERS = ["usado", "seminovo", "novo", "barato"]

DEFAULT_MAX_VARIATIONS = 8


def _normalize(text: str) -> str:
    return text.strip().lower()


def expand(keyword: str, max_variations: int = DEFAULT_MAX_VARIATIONS) -> list[str]:
    """Gera variações da keyword. Retorna lista ordenada por prioridade,
    sem duplicatas, limitada a `max_variations` itens (incluindo a original)."""
    base = _normalize(keyword)
    if not base:
        return []

    out: list[str] = [base]
    seen: set[str] = {base}

    def _add(candidate: str) -> None:
        c = _normalize(candidate)
        if c and c not in seen:
            seen.add(c)
            out.append(c)

    # 1. Versões / modelos (match exato — `base` é a chave)
    for trigger, expansions in VERSION_EXPANSIONS.items():
        if base == trigger or base.startswith(trigger + " "):
            for e in expansions:
                _add(e)

    # 2. Marca explícita
    for trigger, expansions in BRAND_EXPANSIONS.items():
        if base == trigger or trigger in base.split():
            for e in expansions:
                _add(e)

    # 3. Sinônimos
    for token in base.split():
        if token in SYNONYMS:
            for syn in SYNONYMS[token]:
                replaced = base.replace(token, syn)
                _add(replaced)

    # 4. Modificadores genéricos
    for mod in COMMON_MODIFIERS:
        _add(f"{base} {mod}")

    return out[:max_variations]


def expand_with_context(
    keyword: str,
    region: str | None = None,
    max_variations: int = DEFAULT_MAX_VARIATIONS,
) -> list[str]:
    """Variações + contexto regional opcional. Não anexa região às variações
    porque o discover_links já faz isso na query — região é argumento separado.
    Esta função existe para uniformidade da API."""
    return expand(keyword, max_variations=max_variations)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("keyword")
    ap.add_argument("--max", type=int, default=DEFAULT_MAX_VARIATIONS)
    args = ap.parse_args()

    variations = expand(args.keyword, max_variations=args.max)
    for v in variations:
        print(v)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
