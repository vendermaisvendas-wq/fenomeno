"""
Fast analytics: mesmas estatísticas do analytics.py, mas lendo de Parquet
e usando polars ou pandas quando disponível. Fallback para stdlib se
nenhuma lib estiver instalada.

Por quê: analytics.py usa SQLite + statistics.stdlib, o que é fino para
alguns milhares de linhas. Para centenas de milhares, polars lê parquet
em paralelo e roda agregações vetorizadas — ordens de magnitude mais rápido.

Backend é escolhido automaticamente na ordem: polars → pandas → stdlib.
Forçar um backend: --backend polars|pandas|stdlib.

Uso:
    python fast_analytics.py hilux iphone          # top tokens
    python fast_analytics.py --all                  # stats globais
    python fast_analytics.py --keyword moto --json
    python fast_analytics.py --backend stdlib hilux

Pré-requisito: rodar `python data_lake.py sync` antes (o parquet precisa existir
para os backends não-stdlib). Com --backend stdlib lê direto do SQLite.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass

from analytics import compute_stats as _stdlib_compute_stats
from analytics import load_listings as _stdlib_load_listings
from logging_setup import get_logger

log = get_logger("fast_analytics")


@dataclass
class FastStats:
    keyword: str
    count: int
    mean: float | None
    median: float | None
    minimum: float | None
    maximum: float | None
    backend: str


def _detect_backend(force: str | None) -> str:
    if force and force != "auto":
        return force
    for name, mod in [("polars", "polars"), ("pandas", "pandas")]:
        try:
            __import__(mod)
            return name
        except ImportError:
            continue
    return "stdlib"


# --- polars backend --------------------------------------------------------

def _polars_stats(keywords: list[str]) -> list[FastStats]:
    import polars as pl
    from data_lake import LISTINGS_DIR

    parquet_file = LISTINGS_DIR / "listings.parquet"
    if not parquet_file.exists():
        raise FileNotFoundError(
            "data_lake/listings/listings.parquet não existe. "
            "Rode `python data_lake.py sync` antes."
        )

    df = pl.read_parquet(str(parquet_file)).filter(
        (pl.col("is_removed") == 0) & pl.col("current_title").is_not_null()
    )
    # parser de preço — usa price_normalizer em um map
    from price_normalizer import parse as parse_price
    df = df.with_columns(
        pl.col("current_price").map_elements(parse_price, return_dtype=pl.Float64)
        .alias("price_f"),
    ).filter(pl.col("price_f").is_not_null())

    out: list[FastStats] = []
    for kw in keywords:
        kw_l = kw.lower()
        sub = df.filter(
            pl.col("current_title").str.to_lowercase().str.contains(kw_l, literal=True)
        )
        n = sub.height
        if n == 0:
            out.append(FastStats(kw, 0, None, None, None, None, "polars"))
            continue
        prices = sub["price_f"]
        out.append(FastStats(
            keyword=kw, count=n,
            mean=round(float(prices.mean()), 2),
            median=round(float(prices.median()), 2),
            minimum=float(prices.min()),
            maximum=float(prices.max()),
            backend="polars",
        ))
    return out


# --- pandas backend --------------------------------------------------------

def _pandas_stats(keywords: list[str]) -> list[FastStats]:
    import pandas as pd
    from data_lake import LISTINGS_DIR
    from price_normalizer import parse as parse_price

    parquet_file = LISTINGS_DIR / "listings.parquet"
    if not parquet_file.exists():
        raise FileNotFoundError(
            "data_lake/listings/listings.parquet não existe. Rode sync antes."
        )

    df = pd.read_parquet(parquet_file)
    df = df[(df["is_removed"] == 0) & df["current_title"].notna()]
    df["price_f"] = df["current_price"].map(parse_price)
    df = df[df["price_f"].notna()]

    out: list[FastStats] = []
    for kw in keywords:
        sub = df[df["current_title"].str.lower().str.contains(kw.lower(), na=False)]
        n = len(sub)
        if n == 0:
            out.append(FastStats(kw, 0, None, None, None, None, "pandas"))
            continue
        prices = sub["price_f"]
        out.append(FastStats(
            keyword=kw, count=n,
            mean=round(float(prices.mean()), 2),
            median=round(float(prices.median()), 2),
            minimum=float(prices.min()),
            maximum=float(prices.max()),
            backend="pandas",
        ))
    return out


# --- stdlib backend (SQLite) -----------------------------------------------

def _stdlib_stats(keywords: list[str]) -> list[FastStats]:
    listings = _stdlib_load_listings()
    out: list[FastStats] = []
    for kw in keywords:
        s = _stdlib_compute_stats(listings, kw)
        out.append(FastStats(
            keyword=kw, count=s.count,
            mean=s.mean, median=s.median,
            minimum=s.minimum, maximum=s.maximum,
            backend="stdlib",
        ))
    return out


BACKENDS = {
    "polars": _polars_stats,
    "pandas": _pandas_stats,
    "stdlib": _stdlib_stats,
}


def analyze(keywords: list[str], backend: str = "auto") -> list[FastStats]:
    resolved = _detect_backend(backend)
    log.info(f"fast_analytics backend={resolved}")
    return BACKENDS[resolved](keywords)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("keywords", nargs="*")
    ap.add_argument("--backend", choices=["auto", "polars", "pandas", "stdlib"],
                    default="auto")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if not args.keywords:
        print("forneça pelo menos uma keyword")
        return 2

    try:
        stats = analyze(args.keywords, backend=args.backend)
    except (FileNotFoundError, ImportError) as e:
        print(f"erro: {e}")
        return 1

    if args.json:
        print(json.dumps([asdict(s) for s in stats], indent=2, ensure_ascii=False))
        return 0

    header = f"{'keyword':<20} {'n':>5} {'mean':>12} {'median':>12} {'min':>10} {'max':>10} backend"
    print(header)
    print("-" * len(header))
    for s in stats:
        fmt = lambda v: f"{v:,.2f}" if isinstance(v, (int, float)) else "-"
        print(
            f"{s.keyword:<20} {s.count:>5} "
            f"{fmt(s.mean):>12} {fmt(s.median):>12} "
            f"{fmt(s.minimum):>10} {fmt(s.maximum):>10} {s.backend}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
