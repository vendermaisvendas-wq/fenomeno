"""
Exportação de listings para CSV, JSON ou Parquet.

Filtros suportados (opcionais, combináveis):
    --keyword <s>     substring no título (case-insensitive)
    --city <s>        substring em current_location
    --min-score <n>   opportunity_score mínimo
    --min-discount <n>  discount_percentage mínimo
    --exclude-outliers não inclui listings flagados como outlier
    --limit <n>       limite de linhas

Formatos:
    --format csv      (stdlib)
    --format json     (stdlib)
    --format parquet  (requer pyarrow — pip install pyarrow)

Uso:
    python export_data.py --format csv --out exports/todos.csv
    python export_data.py --format json --keyword hilux --out exports/hilux.json
    python export_data.py --format parquet --min-score 60 --out exports/top.parquet
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

from db import connect, init_db
from logging_setup import get_logger, kv

log = get_logger("export")

FIELDS = [
    "id", "url", "source", "first_seen_at", "last_seen_at", "last_status",
    "is_removed", "removed_at", "current_title", "current_price",
    "current_currency", "current_location",
    "estimated_market_value", "discount_percentage", "opportunity_score",
    "cluster_id", "duplicate_group_id", "price_outlier", "fraud_risk_score",
]


def _build_query(
    keyword: str | None,
    city: str | None,
    min_score: int | None,
    min_discount: float | None,
    exclude_outliers: bool,
    limit: int | None,
) -> tuple[str, list[Any]]:
    where = ["1=1"]
    params: list[Any] = []

    if keyword:
        where.append("LOWER(COALESCE(current_title, '')) LIKE ?")
        params.append(f"%{keyword.lower()}%")
    if city:
        where.append("LOWER(COALESCE(current_location, '')) LIKE ?")
        params.append(f"%{city.lower()}%")
    if min_score is not None:
        where.append("COALESCE(opportunity_score, 0) >= ?")
        params.append(min_score)
    if min_discount is not None:
        where.append("COALESCE(discount_percentage, -999) >= ?")
        params.append(min_discount)
    if exclude_outliers:
        where.append("COALESCE(price_outlier, 0) = 0")

    select_cols = ", ".join(FIELDS)
    sql = f"SELECT {select_cols} FROM listings WHERE {' AND '.join(where)}"
    sql += " ORDER BY opportunity_score DESC NULLS LAST, last_seen_at DESC"
    if limit:
        sql += f" LIMIT {int(limit)}"
    return sql, params


def _fetch(**filters) -> list[dict]:
    init_db()
    sql, params = _build_query(**filters)
    with connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


# --- writers ----------------------------------------------------------------

def write_csv(rows: list[dict], out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k) for k in FIELDS})


def write_json(rows: list[dict], out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(rows, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def write_parquet(rows: list[dict], out: Path) -> None:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        raise SystemExit(
            "pyarrow não instalado — `pip install pyarrow` para parquet"
        )
    out.parent.mkdir(parents=True, exist_ok=True)
    # Normaliza: garante que todas as linhas têm as mesmas chaves
    cols: dict[str, list] = {k: [] for k in FIELDS}
    for r in rows:
        for k in FIELDS:
            cols[k].append(r.get(k))
    table = pa.table(cols)
    pq.write_table(table, str(out))


WRITERS = {"csv": write_csv, "json": write_json, "parquet": write_parquet}


# --- CLI --------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--format", choices=list(WRITERS), default="csv")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--keyword")
    ap.add_argument("--city")
    ap.add_argument("--min-score", type=int)
    ap.add_argument("--min-discount", type=float)
    ap.add_argument("--exclude-outliers", action="store_true")
    ap.add_argument("--limit", type=int)
    args = ap.parse_args()

    rows = _fetch(
        keyword=args.keyword,
        city=args.city,
        min_score=args.min_score,
        min_discount=args.min_discount,
        exclude_outliers=args.exclude_outliers,
        limit=args.limit,
    )
    WRITERS[args.format](rows, args.out)
    log.info(kv(event="exported", rows=len(rows), format=args.format,
                out=str(args.out)))
    print(f"exported {len(rows)} rows → {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
