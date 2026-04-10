"""
Parser canônico de preços para o projeto inteiro.

Cobre os formatos comuns no Marketplace BR:

    "R$ 185.000"        → 185000.0
    "R$ 1.234,56"       → 1234.56
    "R$185000"          → 185000.0
    "185000"            → 185000.0
    "185k"              → 185000.0
    "1.5k"              → 1500.0
    "185 mil"           → 185000.0
    "2 milhões"         → 2000000.0
    "2,5 mi"            → 2500000.0
    "1234.56"           → 1234.56

Retorna sempre float ou None. Nunca levanta exceção em input malformado.

Este módulo é pure: sem DB, sem IO, safe para importar em qualquer lugar.
Os módulos antigos (analytics._to_float) continuam funcionando e agora
delegam para este parser.
"""

from __future__ import annotations

import math
import re

# Regex para os sufixos multiplicativos (ordem importa: "mi" antes de "m")
_SUFFIX_K = re.compile(r"^\s*(-?\d+(?:[.,]\d+)?)\s*k\s*$", re.IGNORECASE)
_SUFFIX_MIL = re.compile(r"^\s*(-?\d+(?:[.,]\d+)?)\s*mil\s*$", re.IGNORECASE)
_SUFFIX_MILHOES = re.compile(
    r"^\s*(-?\d+(?:[.,]\d+)?)\s*(?:mi|mil[hõo]?[ãeoõ]?s?|milhoes|milhões|milhao|milhão)\s*$",
    re.IGNORECASE,
)

_CURRENCY_RE = re.compile(r"(?:r\$|brl|usd|us\$|\$|€|eur)", re.IGNORECASE)


def _to_number(num_str: str) -> float | None:
    """Parseia uma substring numérica BR/intl em float. Não aplica sufixo."""
    s = num_str.strip()
    if not s:
        return None

    # Remove tudo que não é dígito, ponto ou vírgula
    s = "".join(c for c in s if c.isdigit() or c in ".,-")
    if not s or s in "-":
        return None

    if "," in s:
        # Formato BR: vírgula é decimal, ponto é milhar
        s = s.replace(".", "").replace(",", ".")
    elif "." in s:
        # Ambíguo: "185.000" (milhar BR) vs "1.50" (decimal)
        parts = s.split(".")
        if len(parts) > 2 or (len(parts) == 2 and len(parts[1]) == 3):
            # Múltiplos pontos OU grupo final de 3 dígitos → milhar
            s = s.replace(".", "")

    try:
        v = float(s)
    except ValueError:
        return None
    return v if math.isfinite(v) else None


def parse(raw: str | int | float | None) -> float | None:
    """Retorna preço em float > 0, ou None."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw) if raw > 0 and math.isfinite(raw) else None

    text = str(raw).strip()
    if not text:
        return None

    # Remove símbolo de moeda mas preserva espaço para os regexes de sufixo
    text = _CURRENCY_RE.sub("", text).strip()

    # Sufixo "k"
    m = _SUFFIX_K.match(text)
    if m:
        v = _to_number(m.group(1))
        return v * 1_000 if v and v > 0 else None

    # Sufixo "mil" (mas NÃO "milhão/mi") — _SUFFIX_MIL exige "mil" exato no fim
    m = _SUFFIX_MIL.match(text)
    if m:
        v = _to_number(m.group(1))
        return v * 1_000 if v and v > 0 else None

    # Sufixo "mi"/"milhão"/"milhões"
    m = _SUFFIX_MILHOES.match(text)
    if m:
        v = _to_number(m.group(1))
        return v * 1_000_000 if v and v > 0 else None

    # Sem sufixo → parser numérico puro
    v = _to_number(text)
    if v is None:
        return None
    return v if v > 0 else None
