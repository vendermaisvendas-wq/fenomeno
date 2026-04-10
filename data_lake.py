"""
Camada híbrida: SQLite é a fonte canônica para ingestão/monitor; Parquet é
o storage otimizado para análise histórica.

Por quê: scans full-table de `snapshots` e `price_history` em SQLite ficam
pesados conforme o banco cresce (centenas de milhares de linhas). Parquet
com compressão + filtro colunar resolve para leitura analítica.

Layout em disco:
    data_lake/
        listings/           snapshot atual das listings (sobrescrito a cada sync)
            listings.parquet
        snapshots/          append-only, particionado por dia (YYYY-MM-DD)
            dt=YYYY-MM-DD/snapshots.parquet
        price_history/      append-only, particionado por dia
            dt=YYYY-MM-DD/price_history.parquet
        _sync_state.json    último sync (ts + contadores)

Funções principais:
    sync_parquet()          SQLite → Parquet (incremental via watermark)
    load_dataset(name)      Parquet → lista de dicts (stdlib)
    query_dataset(name, ..) filtros simples

pyarrow é dependência opcional. Sem ele, o módulo importa mas cada função
levanta `ImportError` com mensagem clara dizendo `pip install pyarrow`.

Uso:
    python data_lake.py sync                      # sincroniza incremental
    python data_lake.py sync --full               # reexporta tudo
    python data_lake.py info                      # mostra watermark + tamanhos
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from db import connect, init_db
from logging_setup import get_logger, kv

log = get_logger("data_lake")

LAKE_DIR = Path("data_lake")
LISTINGS_DIR = LAKE_DIR / "listings"
SNAPSHOTS_DIR = LAKE_DIR / "snapshots"
PRICE_HISTORY_DIR = LAKE_DIR / "price_history"
STATE_FILE = LAKE_DIR / "_sync_state.json"


def _require_pyarrow():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        return pa, pq
    except ImportError as e:
        raise ImportError(
            "data_lake requer pyarrow. Instale com: pip install pyarrow"
        ) from e


@dataclass
class SyncState:
    last_snapshot_at: str | None = None
    last_price_history_at: str | None = None
    last_full_sync_at: str | None = None
    listings_count: int = 0
    snapshots_count: int = 0
    price_history_count: int = 0


def load_state() -> SyncState:
    if not STATE_FILE.exists():
        return SyncState()
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        return SyncState(**data)
    except (json.JSONDecodeError, TypeError):
        return SyncState()


def save_state(state: SyncState) -> None:
    LAKE_DIR.mkdir(exist_ok=True)
    STATE_FILE.write_text(
        json.dumps(asdict(state), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# --- sync ----------------------------------------------------------------

def _write_parquet(rows: list[dict], out: Path, schema_cols: list[str]) -> int:
    """Escreve uma lista de dicts em parquet. Retorna número de linhas."""
    if not rows:
        return 0
    pa, pq = _require_pyarrow()
    out.parent.mkdir(parents=True, exist_ok=True)
    # Normaliza: garantir todas as chaves nas colunas, None onde ausente
    cols: dict[str, list] = {k: [] for k in schema_cols}
    for r in rows:
        for k in schema_cols:
            cols[k].append(r.get(k))
    table = pa.table(cols)
    pq.write_table(table, str(out), compression="snappy")
    return len(rows)


LISTINGS_COLS = [
    "id", "url", "source", "first_seen_at", "last_seen_at", "last_status",
    "is_removed", "removed_at", "reappeared_at",
    "current_title", "current_price", "current_currency", "current_location",
    "estimated_market_value", "discount_percentage", "opportunity_score",
    "cluster_id", "duplicate_group_id", "price_outlier", "fraud_risk_score",
    "current_seller", "predicted_price", "price_gap",
    "liquidity_score", "seller_reliability_score",
]

SNAPSHOT_COLS = [
    "id", "listing_id", "fetched_at", "status", "payload_hash", "payload_json"
]

PRICE_HIST_COLS = [
    "id", "listing_id", "price", "price_raw", "currency", "recorded_at"
]


def _sync_listings_snapshot() -> int:
    """listings é tabela estado-atual → full dump toda vez (não incremental)."""
    with connect() as conn:
        rows = [dict(r) for r in conn.execute(
            f"SELECT {', '.join(LISTINGS_COLS)} FROM listings"
        ).fetchall()]
    return _write_parquet(rows, LISTINGS_DIR / "listings.parquet", LISTINGS_COLS)


def _day_partition_path(base: Path, day: str) -> Path:
    return base / f"dt={day}" / f"{base.name}.parquet"


def _sync_incremental(
    table: str, cols: list[str], ts_col: str, base_dir: Path,
    watermark: str | None, full: bool,
) -> tuple[int, str | None]:
    """Faz dump incremental (append-only) particionado por dia do ts_col.
    Retorna (count_total, novo_watermark)."""
    with connect() as conn:
        if full or watermark is None:
            rows = [dict(r) for r in conn.execute(
                f"SELECT {', '.join(cols)} FROM {table} ORDER BY {ts_col} ASC"
            ).fetchall()]
        else:
            rows = [dict(r) for r in conn.execute(
                f"SELECT {', '.join(cols)} FROM {table} "
                f"WHERE {ts_col} > ? ORDER BY {ts_col} ASC",
                (watermark,),
            ).fetchall()]

    if not rows:
        return 0, watermark

    # Particiona por dia (YYYY-MM-DD do ts_col)
    by_day: dict[str, list[dict]] = {}
    for r in rows:
        ts = r.get(ts_col) or ""
        day = ts[:10] or "_nodate"
        by_day.setdefault(day, []).append(r)

    total = 0
    for day, day_rows in by_day.items():
        out = _day_partition_path(base_dir, day)
        # Modo "append": se a partição já existe, concatena via leitura + rewrite
        # (parquet não tem append nativo; para simplicidade aceitamos rewrite)
        existing = _load_parquet(out) if out.exists() else []
        seen_ids = {r.get("id") for r in existing if "id" in r}
        new_rows = [r for r in day_rows if r.get("id") not in seen_ids]
        combined = existing + new_rows
        total += _write_parquet(combined, out, cols) - len(existing)

    new_watermark = rows[-1].get(ts_col) or watermark
    return total, new_watermark


def sync_parquet(full: bool = False) -> dict:
    """Sincroniza SQLite → Parquet. Incremental por default."""
    init_db()
    _require_pyarrow()
    LAKE_DIR.mkdir(exist_ok=True)
    state = load_state()

    listings_n = _sync_listings_snapshot()

    snap_n, snap_wm = _sync_incremental(
        "snapshots", SNAPSHOT_COLS, "fetched_at", SNAPSHOTS_DIR,
        state.last_snapshot_at, full,
    )
    ph_n, ph_wm = _sync_incremental(
        "price_history", PRICE_HIST_COLS, "recorded_at", PRICE_HISTORY_DIR,
        state.last_price_history_at, full,
    )

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    new_state = SyncState(
        last_snapshot_at=snap_wm,
        last_price_history_at=ph_wm,
        last_full_sync_at=now if full else state.last_full_sync_at,
        listings_count=listings_n,
        snapshots_count=state.snapshots_count + snap_n,
        price_history_count=state.price_history_count + ph_n,
    )
    save_state(new_state)

    result = {
        "listings_rewritten": listings_n,
        "snapshots_added": snap_n,
        "price_history_added": ph_n,
        "full": full,
    }
    log.info(kv(event="parquet_synced", **result))
    return result


# --- load/query ---------------------------------------------------------

def _load_parquet(path: Path) -> list[dict]:
    if not path.exists():
        return []
    _, pq = _require_pyarrow()
    table = pq.read_table(str(path))
    return table.to_pylist()


def load_dataset(name: str) -> list[dict]:
    """Carrega um dataset completo do lake. `name` ∈ {listings, snapshots, price_history}."""
    if name == "listings":
        return _load_parquet(LISTINGS_DIR / "listings.parquet")

    if name not in ("snapshots", "price_history"):
        raise ValueError(f"unknown dataset: {name}")

    base = SNAPSHOTS_DIR if name == "snapshots" else PRICE_HISTORY_DIR
    if not base.exists():
        return []
    out: list[dict] = []
    for part in sorted(base.glob("dt=*")):
        pfile = part / f"{base.name}.parquet"
        if pfile.exists():
            out.extend(_load_parquet(pfile))
    return out


def query_dataset(name: str, **filters) -> list[dict]:
    """Filtros simples sobre um dataset. Filtros aceitos: min_<col>, max_<col>,
    eq_<col>, contains_<col> (substring case-insensitive em string)."""
    rows = load_dataset(name)
    out = []
    for r in rows:
        keep = True
        for k, v in filters.items():
            if k.startswith("min_"):
                col = k[4:]
                val = r.get(col)
                if val is None or val < v:
                    keep = False
                    break
            elif k.startswith("max_"):
                col = k[4:]
                val = r.get(col)
                if val is None or val > v:
                    keep = False
                    break
            elif k.startswith("eq_"):
                col = k[3:]
                if r.get(col) != v:
                    keep = False
                    break
            elif k.startswith("contains_"):
                col = k[9:]
                val = r.get(col) or ""
                if str(v).lower() not in str(val).lower():
                    keep = False
                    break
        if keep:
            out.append(r)
    return out


# --- CLI ----------------------------------------------------------------

def info() -> dict[str, Any]:
    state = load_state()
    out: dict[str, Any] = asdict(state)
    for name, base in (("listings", LISTINGS_DIR),
                       ("snapshots", SNAPSHOTS_DIR),
                       ("price_history", PRICE_HISTORY_DIR)):
        total_size = sum(
            p.stat().st_size for p in base.rglob("*.parquet") if p.is_file()
        ) if base.exists() else 0
        files = sum(1 for _ in base.rglob("*.parquet")) if base.exists() else 0
        out[f"{name}_files"] = files
        out[f"{name}_bytes"] = total_size
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    sp_sync = sub.add_parser("sync", help="sincroniza SQLite → Parquet")
    sp_sync.add_argument("--full", action="store_true",
                         help="rewrite completo ao invés de incremental")

    sub.add_parser("info", help="mostra estado do data lake")

    args = ap.parse_args()

    if args.cmd == "sync":
        try:
            result = sync_parquet(full=args.full)
        except ImportError as e:
            print(f"erro: {e}")
            return 2
        for k, v in result.items():
            print(f"  {k:25s} {v}")
        return 0

    if args.cmd == "info":
        for k, v in info().items():
            print(f"  {k:25s} {v}")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
