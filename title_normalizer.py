"""
Normalização e tokenização de títulos para agrupamento de anúncios semelhantes.

Funções expostas (todas puras, sem DB):

    normalize(text)       -> lowercase, sem acento, sem pontuação redundante
    tokens(text)          -> set de tokens normalizados (≥2 chars, sem stopwords)
    extract_year(text)    -> int ou None (primeiro ano 1980-2049 encontrado)
    extract_brand(text)   -> str ou None (primeira marca conhecida no título)
    signature(title)      -> (brand, frozenset(model_tokens), year) p/ agrupamento
    jaccard(a, b)         -> similaridade entre dois sets de tokens

Objetivo: dar ao market_value e ao duplicate_detector uma base consistente
para decidir "dois anúncios são comparáveis".
"""

from __future__ import annotations

import re
import unicodedata

# Stopwords PT-BR relevantes para anúncios (marketplace-specific)
STOPWORDS_PT = {
    "a", "o", "as", "os", "um", "uma", "de", "do", "da", "dos", "das",
    "para", "por", "com", "sem", "em", "no", "na", "nos", "nas",
    "e", "ou", "mas", "muito", "muita", "mais", "menos",
    "ao", "aos", "ja", "so", "ate", "sobre", "entre", "tao", "que",
    # vocabulário típico de marketplace
    "vendo", "vende", "aceito", "troco", "troca", "trocas",
    "barato", "baratissimo", "novo", "nova", "usado", "usada",
    "bom", "boa", "otimo", "otima", "excelente", "estado", "conservado",
    "conservada", "impecavel", "perfeito", "perfeita",
    "r", "rs", "reais", "real", "valor", "preco",
    "urgente", "urgencia", "hoje", "desapego",
}

# Marcas conhecidas — comparáveis só deveriam atravessar marcas iguais
VEHICLE_BRANDS = {
    "toyota", "honda", "yamaha", "chevrolet", "chevy", "gm", "ford",
    "volkswagen", "vw", "fiat", "hyundai", "kia", "nissan", "renault",
    "peugeot", "citroen", "mitsubishi", "jeep", "bmw", "mercedes",
    "audi", "volvo", "suzuki", "kawasaki", "ducati", "harley",
}
ELECTRONICS_BRANDS = {
    "apple", "iphone", "ipad", "macbook", "airpods",
    "samsung", "xiaomi", "motorola", "lg", "sony",
    "dell", "lenovo", "hp", "asus", "acer", "positivo",
    "playstation", "ps4", "ps5", "xbox", "nintendo", "switch",
}
KNOWN_BRANDS = VEHICLE_BRANDS | ELECTRONICS_BRANDS

YEAR_RE = re.compile(r"\b(19[89]\d|20[0-4]\d)\b")
TOKEN_RE = re.compile(r"[a-z0-9]+")


def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


def normalize(text: str | None) -> str:
    """Lowercase + sem acento + strip. Mantém pontuação para extract_year."""
    if not text:
        return ""
    return _strip_accents(text.lower()).strip()


def tokens(text: str | None) -> set[str]:
    """Tokens alfanuméricos com ≥2 chars, sem stopwords PT-BR."""
    n = normalize(text)
    return {
        t for t in TOKEN_RE.findall(n)
        if len(t) >= 2 and t not in STOPWORDS_PT
    }


def extract_year(text: str | None) -> int | None:
    """Primeiro ano entre 1980 e 2049 encontrado no texto original."""
    if not text:
        return None
    m = YEAR_RE.search(text)
    return int(m.group(0)) if m else None


def extract_brand(text: str | None) -> str | None:
    """Primeira marca conhecida presente nos tokens normalizados."""
    toks = tokens(text)
    # determinístico: iterar em ordem alfabética, retornar primeiro match
    for brand in sorted(toks & KNOWN_BRANDS):
        return brand
    return None


def signature(title: str | None) -> tuple[str | None, frozenset[str], int | None]:
    """(brand, model_tokens_without_year_and_brand, year)"""
    year = extract_year(title)
    brand = extract_brand(title)
    model_toks = tokens(title)
    if year is not None:
        model_toks.discard(str(year))
    if brand is not None:
        model_toks.discard(brand)
    return (brand, frozenset(model_toks), year)


def jaccard(a: set[str], b: set[str]) -> float:
    """Similaridade Jaccard. Vazios ∩ vazios → 1.0 (convenção)."""
    if not a and not b:
        return 1.0
    union = a | b
    if not union:
        return 0.0
    return len(a & b) / len(union)
