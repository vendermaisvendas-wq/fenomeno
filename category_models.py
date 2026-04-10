"""
Classificação de anúncios em categorias + estatísticas por categoria.

Categorias atuais (rule-based, heurística por tokens):
    vehicles      carros, motos, caminhões
    electronics   celulares, notebooks, tvs, consoles
    real_estate   casas, apartamentos, terrenos
    furniture     móveis, eletrodomésticos
    other         (fallback)

O classificador soma hits por categoria sobre os tokens do título (após
stopwords). Empate: ordem fixa (vehicles > electronics > real_estate > furniture).

Depois de classificar, cada categoria ganha estatísticas próprias (médias,
contagens, desconto médio) via `category_stats()`. Isso alimenta o dashboard
e permite que o market_value segmente comparáveis por categoria no futuro.

Não treinamos modelo sklearn aqui — a categoria é estrutural (o input é um
título pequeno com alta variância), e uma classificação rule-based com
vocabulário curado é suficiente e explicável.

Uso:
    python category_models.py                    # classifica + stats
    python category_models.py --classify-only
    python category_models.py --stats-only
"""

from __future__ import annotations

import argparse
import statistics
from collections import Counter
from dataclasses import asdict, dataclass

from db import connect, init_db
from logging_setup import get_logger, kv
from price_normalizer import parse as parse_price
from title_normalizer import tokens

log = get_logger("category_models")

# Vocabulário por categoria (lowercase, sem acento — casa com title_normalizer.tokens)
VEHICLES_TOKENS = {
    "carro", "carros", "moto", "motocicleta", "caminhao", "caminhoes",
    "caminhonete", "pickup", "utilitario", "suv", "sedan", "hatch",
    "kombi", "van", "veiculo",
    # marcas
    "toyota", "honda", "yamaha", "chevrolet", "chevy", "gm", "ford",
    "volkswagen", "vw", "fiat", "hyundai", "kia", "nissan", "renault",
    "peugeot", "citroen", "mitsubishi", "jeep", "bmw", "mercedes", "audi",
    "volvo", "suzuki", "kawasaki", "ducati",
    # modelos populares
    "hilux", "civic", "corolla", "onix", "gol", "uno", "palio", "ka",
    "strada", "fiesta", "cg", "titan", "biz", "fazer", "cb", "xre",
    "factor", "cruze", "s10", "ranger",
    # combustíveis
    "diesel", "gasolina", "flex", "etanol", "gnv",
}

ELECTRONICS_TOKENS = {
    "celular", "smartphone", "telefone", "telefonia",
    "iphone", "ipad", "macbook", "airpods", "apple",
    "samsung", "xiaomi", "motorola", "moto", "redmi", "poco",
    "lg", "sony", "huawei", "nokia", "lenovo", "dell", "hp",
    "asus", "acer", "positivo",
    "notebook", "laptop", "pc", "desktop", "monitor",
    "tv", "televisao", "smart",
    "playstation", "ps4", "ps5", "xbox", "nintendo", "switch",
    "console", "controle", "joystick",
    "fone", "headset", "caixa",
    "tablet",
}

REAL_ESTATE_TOKENS = {
    "casa", "apto", "apartamento", "kitnet", "studio", "quitinete",
    "cobertura", "sobrado", "chacara", "sitio", "fazenda", "rancho",
    "terreno", "lote", "area", "galpao", "loja", "sala", "imovel",
    "comercial", "residencial", "alugar", "aluguel", "aluga",
    "financiamento", "financiado", "escritura",
    "quartos", "suites", "vagas",
}

FURNITURE_TOKENS = {
    "mesa", "cadeira", "cadeiras", "sofa", "poltrona", "rack", "cristaleira",
    "guarda", "roupeiro", "armario", "cama", "colchao", "cabeceira",
    "estante", "escrivaninha", "criado", "comoda",
    # eletrodomésticos
    "geladeira", "freezer", "fogao", "microondas", "lavadora",
    "lava", "louca", "secadora", "forno", "cooktop", "depurador",
    "ventilador", "ar", "condicionado", "climatizador",
    # utensílios grandes
    "panela", "liquidificador", "batedeira",
}

CATEGORIES = {
    "vehicles":    VEHICLES_TOKENS,
    "electronics": ELECTRONICS_TOKENS,
    "real_estate": REAL_ESTATE_TOKENS,
    "furniture":   FURNITURE_TOKENS,
}

# Ordem de desempate (primeiro vence em caso de empate)
CATEGORY_PRIORITY = ["vehicles", "electronics", "real_estate", "furniture"]


def classify(title: str | None) -> str:
    """Devolve categoria ou 'other'. Puro, sem DB."""
    if not title:
        return "other"
    toks = tokens(title)
    if not toks:
        return "other"

    scores: dict[str, int] = {}
    for cat, vocab in CATEGORIES.items():
        hits = len(toks & vocab)
        if hits:
            scores[cat] = hits

    if not scores:
        return "other"

    max_score = max(scores.values())
    winners = [c for c, s in scores.items() if s == max_score]
    if len(winners) == 1:
        return winners[0]
    # Desempate por prioridade fixa
    for cat in CATEGORY_PRIORITY:
        if cat in winners:
            return cat
    return winners[0]


# --- apply -------------------------------------------------------------

def apply_classification(dry_run: bool = False) -> dict:
    init_db()
    updates: list[tuple[str, str]] = []
    counts: Counter[str] = Counter()

    with connect() as conn:
        rows = conn.execute(
            "SELECT id, current_title FROM listings WHERE current_title IS NOT NULL"
        ).fetchall()
        for r in rows:
            cat = classify(r["current_title"])
            counts[cat] += 1
            updates.append((cat, r["id"]))

        if not dry_run and updates:
            conn.executemany(
                "UPDATE listings SET category = ? WHERE id = ?",
                updates,
            )

    log.info(kv(event="categories_applied", **dict(counts)))
    return {"classified": len(updates), "distribution": dict(counts)}


# --- stats -------------------------------------------------------------

@dataclass
class CategoryStats:
    category: str
    total: int
    active: int
    avg_price: float | None
    median_price: float | None
    avg_discount: float | None
    avg_liquidity: float | None


def category_stats() -> list[CategoryStats]:
    init_db()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT category, is_removed, current_price,
                   discount_percentage, liquidity_score
              FROM listings
             WHERE category IS NOT NULL
            """
        ).fetchall()

    buckets: dict[str, list[dict]] = {}
    for r in rows:
        buckets.setdefault(r["category"], []).append(dict(r))

    out: list[CategoryStats] = []
    for cat, items in buckets.items():
        prices = [parse_price(i["current_price"]) for i in items]
        prices = [p for p in prices if p is not None]
        discounts = [i["discount_percentage"] for i in items
                     if i["discount_percentage"] is not None]
        liqs = [i["liquidity_score"] for i in items
                if i["liquidity_score"] is not None]

        out.append(CategoryStats(
            category=cat,
            total=len(items),
            active=sum(1 for i in items if not i["is_removed"]),
            avg_price=round(statistics.fmean(prices), 2) if prices else None,
            median_price=round(statistics.median(prices), 2) if prices else None,
            avg_discount=round(statistics.fmean(discounts), 2) if discounts else None,
            avg_liquidity=round(statistics.fmean(liqs), 2) if liqs else None,
        ))

    order = {c: i for i, c in enumerate(CATEGORY_PRIORITY + ["other"])}
    out.sort(key=lambda s: order.get(s.category, 999))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--classify-only", action="store_true")
    ap.add_argument("--stats-only", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not args.stats_only:
        result = apply_classification(dry_run=args.dry_run)
        print(f"classified: {result['classified']}")
        print(f"distribution: {result['distribution']}")

    if not args.classify_only:
        stats = category_stats()
        print()
        print(f"{'category':<15s} {'total':>7s} {'active':>7s} "
              f"{'avg_price':>13s} {'avg_disc':>10s} {'avg_liq':>9s}")
        print("-" * 70)
        for s in stats:
            ap_s = f"{s.avg_price:,.0f}" if s.avg_price else "-"
            ad_s = f"{s.avg_discount:.1f}%" if s.avg_discount is not None else "-"
            aliq = f"{s.avg_liquidity:.1f}" if s.avg_liquidity is not None else "-"
            print(f"{s.category:<15s} {s.total:>7d} {s.active:>7d} "
                  f"{ap_s:>13s} {ad_s:>10s} {aliq:>9s}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
