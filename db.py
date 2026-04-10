"""
Camada de banco central. Todos os outros módulos devem ler/escrever aqui
para garantir schema único.

Schema é idempotente: chamar `init_db()` em qualquer versão existente só
adiciona o que falta (CREATE IF NOT EXISTS).
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

DB_PATH = Path("marketplace.sqlite3")

SCHEMA = """
CREATE TABLE IF NOT EXISTS listings (
    id                      TEXT PRIMARY KEY,
    url                     TEXT NOT NULL,
    source                  TEXT,
    first_seen_at           TEXT NOT NULL,
    last_seen_at            TEXT NOT NULL,
    last_status             TEXT NOT NULL,
    is_removed              INTEGER NOT NULL DEFAULT 0,
    removed_at              TEXT,
    reappeared_at           TEXT,
    current_title           TEXT,
    current_price           TEXT,
    current_currency        TEXT,
    current_location        TEXT,
    estimated_market_value  REAL,
    discount_percentage     REAL,
    opportunity_score       INTEGER,
    cluster_id              INTEGER,
    duplicate_group_id      INTEGER,
    price_outlier           INTEGER,
    fraud_risk_score        INTEGER,
    current_seller          TEXT,
    predicted_price         REAL,
    price_gap               REAL,
    liquidity_score         INTEGER,
    seller_reliability_score INTEGER,
    city                    TEXT,
    state                   TEXT,
    category                TEXT,
    repost_count            INTEGER NOT NULL DEFAULT 0,
    fresh_opportunity_score INTEGER
);
CREATE INDEX IF NOT EXISTS idx_listings_last_seen ON listings(last_seen_at);
CREATE INDEX IF NOT EXISTS idx_listings_status    ON listings(last_status);
CREATE INDEX IF NOT EXISTS idx_listings_removed   ON listings(is_removed);

CREATE TABLE IF NOT EXISTS snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id      TEXT NOT NULL,
    fetched_at      TEXT NOT NULL,
    status          TEXT NOT NULL,
    payload_hash    TEXT NOT NULL,
    payload_json    TEXT NOT NULL,
    FOREIGN KEY (listing_id) REFERENCES listings(id)
);
CREATE INDEX IF NOT EXISTS idx_snap_listing     ON snapshots(listing_id, fetched_at);
CREATE INDEX IF NOT EXISTS idx_snap_fetched_at  ON snapshots(fetched_at);

CREATE TABLE IF NOT EXISTS events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id      TEXT NOT NULL,
    at              TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    old_value       TEXT,
    new_value       TEXT,
    FOREIGN KEY (listing_id) REFERENCES listings(id)
);
CREATE INDEX IF NOT EXISTS idx_events_listing ON events(listing_id, at);
CREATE INDEX IF NOT EXISTS idx_events_type    ON events(event_type, at);

CREATE TABLE IF NOT EXISTS price_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id      TEXT NOT NULL,
    price           REAL NOT NULL,
    price_raw       TEXT,                         -- string original, pré-parsing
    currency        TEXT,
    recorded_at     TEXT NOT NULL,
    FOREIGN KEY (listing_id) REFERENCES listings(id)
);
CREATE INDEX IF NOT EXISTS idx_ph_listing  ON price_history(listing_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_ph_price    ON price_history(price);
CREATE INDEX IF NOT EXISTS idx_ph_recorded ON price_history(recorded_at);

CREATE TABLE IF NOT EXISTS parser_health_history (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    at           TEXT NOT NULL,
    sample_size  INTEGER NOT NULL,
    ok_rate      REAL,
    jsonld_rate  REAL,
    og_rate      REAL,
    relay_rate   REAL,
    dom_rate     REAL,
    verdict      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_phh_at ON parser_health_history(at);

CREATE TABLE IF NOT EXISTS seller_stats (
    seller_name         TEXT PRIMARY KEY,
    total_listings      INTEGER NOT NULL,
    active_listings     INTEGER NOT NULL,
    removed_listings    INTEGER NOT NULL,
    duplicate_count     INTEGER NOT NULL,
    avg_price           REAL,
    avg_opportunity     REAL,
    reliability_score   INTEGER NOT NULL,
    computed_at         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ss_reliability ON seller_stats(reliability_score);

CREATE TABLE IF NOT EXISTS geo_coverage (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    city             TEXT NOT NULL,
    state            TEXT,
    listings_count   INTEGER NOT NULL,
    active_count     INTEGER NOT NULL,
    avg_price        REAL,
    avg_discount     REAL,
    coverage_score   INTEGER NOT NULL,
    computed_at      TEXT NOT NULL,
    UNIQUE(city, state)
);
CREATE INDEX IF NOT EXISTS idx_geo_state    ON geo_coverage(state);
CREATE INDEX IF NOT EXISTS idx_geo_coverage ON geo_coverage(coverage_score);

CREATE TABLE IF NOT EXISTS market_density (
    token             TEXT PRIMARY KEY,
    total_listings    INTEGER NOT NULL,
    active_listings   INTEGER NOT NULL,
    removed_listings  INTEGER NOT NULL,
    removal_rate      REAL,
    avg_velocity_days REAL,
    competition_score INTEGER NOT NULL,
    computed_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_md_competition ON market_density(competition_score);

CREATE TABLE IF NOT EXISTS watchers (
    watch_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      TEXT,
    keyword      TEXT NOT NULL,
    region       TEXT,
    min_price    REAL,
    max_price    REAL,
    is_active    INTEGER NOT NULL DEFAULT 1,
    priority     INTEGER NOT NULL DEFAULT 2,
    last_run_at  TEXT,
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_watchers_active   ON watchers(is_active);
CREATE INDEX IF NOT EXISTS idx_watchers_user     ON watchers(user_id);
CREATE INDEX IF NOT EXISTS idx_watchers_priority ON watchers(priority);

CREATE TABLE IF NOT EXISTS watcher_results (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    watch_id            INTEGER NOT NULL,
    listing_id          TEXT NOT NULL,
    first_seen          TEXT NOT NULL,
    is_initial_backfill INTEGER NOT NULL DEFAULT 0,
    UNIQUE(watch_id, listing_id),
    FOREIGN KEY (watch_id) REFERENCES watchers(watch_id)
);
CREATE INDEX IF NOT EXISTS idx_wr_watch   ON watcher_results(watch_id);
CREATE INDEX IF NOT EXISTS idx_wr_listing ON watcher_results(listing_id);

CREATE TABLE IF NOT EXISTS discovery_cache (
    query_hash  TEXT PRIMARY KEY,
    query_text  TEXT NOT NULL,
    region      TEXT,
    result_json TEXT NOT NULL,
    expires_at  TEXT NOT NULL,
    created_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dc_expires ON discovery_cache(expires_at);

CREATE TABLE IF NOT EXISTS discovery_graph (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_query      TEXT,
    child_query       TEXT NOT NULL,
    source_listing_id TEXT,
    depth             INTEGER NOT NULL DEFAULT 0,
    created_at        TEXT NOT NULL,
    UNIQUE(parent_query, child_query)
);
CREATE INDEX IF NOT EXISTS idx_dg_child  ON discovery_graph(child_query);
CREATE INDEX IF NOT EXISTS idx_dg_depth  ON discovery_graph(depth);
CREATE INDEX IF NOT EXISTS idx_dg_parent ON discovery_graph(parent_query);
"""

# Índices que referenciam colunas adicionadas por migração — aplicados
# depois que _migrate_columns() roda.
SCHEMA_V4_INDICES = """
CREATE INDEX IF NOT EXISTS idx_listings_score     ON listings(opportunity_score);
CREATE INDEX IF NOT EXISTS idx_listings_cluster   ON listings(cluster_id);
CREATE INDEX IF NOT EXISTS idx_listings_dupgroup  ON listings(duplicate_group_id);
CREATE INDEX IF NOT EXISTS idx_listings_outlier   ON listings(price_outlier);
CREATE INDEX IF NOT EXISTS idx_listings_fraud     ON listings(fraud_risk_score);
CREATE INDEX IF NOT EXISTS idx_listings_seller    ON listings(current_seller);
CREATE INDEX IF NOT EXISTS idx_listings_liquidity ON listings(liquidity_score);
CREATE INDEX IF NOT EXISTS idx_listings_gap       ON listings(price_gap);
CREATE INDEX IF NOT EXISTS idx_listings_city      ON listings(city);
CREATE INDEX IF NOT EXISTS idx_listings_state     ON listings(state);
CREATE INDEX IF NOT EXISTS idx_listings_category  ON listings(category);
CREATE INDEX IF NOT EXISTS idx_listings_repost    ON listings(repost_count);
CREATE INDEX IF NOT EXISTS idx_listings_fresh     ON listings(fresh_opportunity_score);
CREATE INDEX IF NOT EXISTS idx_listings_oppprob   ON listings(opportunity_probability);
CREATE INDEX IF NOT EXISTS idx_watchers_plan      ON watchers(plan);
"""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@contextmanager
def connect(db_path: Path | str = DB_PATH) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _migrate_columns(conn: sqlite3.Connection) -> list[str]:
    """Aplica ALTER TABLE ADD COLUMN para colunas novas em bancos legados.
    Retorna lista de nomes adicionados para fins de log."""
    added: list[str] = []

    def _add(table: str, col: str, col_type: str) -> None:
        existing = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if col not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
            added.append(f"{table}.{col}")

    # Colunas de inteligência (v4)
    _add("listings", "estimated_market_value", "REAL")
    _add("listings", "discount_percentage", "REAL")
    _add("listings", "opportunity_score", "INTEGER")
    _add("listings", "cluster_id", "INTEGER")
    # v5
    _add("listings", "duplicate_group_id", "INTEGER")
    _add("listings", "price_outlier", "INTEGER")
    _add("listings", "fraud_risk_score", "INTEGER")
    # v6
    _add("listings", "current_seller", "TEXT")
    _add("listings", "predicted_price", "REAL")
    _add("listings", "price_gap", "REAL")
    _add("listings", "liquidity_score", "INTEGER")
    _add("listings", "seller_reliability_score", "INTEGER")
    # v7
    _add("listings", "city", "TEXT")
    _add("listings", "state", "TEXT")
    _add("listings", "category", "TEXT")
    _add("listings", "repost_count", "INTEGER NOT NULL DEFAULT 0")
    _add("listings", "fresh_opportunity_score", "INTEGER")
    # v9
    _add("listings", "very_recent_listing", "INTEGER NOT NULL DEFAULT 0")
    _add("watchers", "priority", "INTEGER NOT NULL DEFAULT 2")
    # v10
    _add("watchers", "plan", "TEXT")
    _add("listings", "opportunity_probability", "REAL")
    return added


def init_db(db_path: Path | str = DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)
        _migrate_columns(conn)
        conn.executescript(SCHEMA_V4_INDICES)


def vacuum_database(db_path: Path | str = DB_PATH) -> dict:
    """Roda VACUUM + ANALYZE. Retorna métricas antes/depois."""
    path = Path(db_path)
    size_before = path.stat().st_size if path.exists() else 0
    # VACUUM não pode rodar dentro de transação — usar conexão direta
    conn = sqlite3.connect(str(path))
    try:
        conn.isolation_level = None  # autocommit
        conn.execute("VACUUM")
        conn.execute("ANALYZE")
    finally:
        conn.close()
    size_after = path.stat().st_size
    return {
        "size_before": size_before,
        "size_after": size_after,
        "bytes_reclaimed": size_before - size_after,
    }


# --- inserts ----------------------------------------------------------------

def discover_insert(
    conn: sqlite3.Connection, listing_id: str, url: str, source: str
) -> bool:
    existing = conn.execute(
        "SELECT id FROM listings WHERE id = ?", (listing_id,)
    ).fetchone()
    if existing:
        return False
    now = now_iso()
    conn.execute(
        """
        INSERT INTO listings (id, url, source, first_seen_at, last_seen_at, last_status)
        VALUES (?, ?, ?, ?, ?, 'pending')
        """,
        (listing_id, url, source, now, now),
    )
    return True


def insert_snapshot(
    conn: sqlite3.Connection,
    listing_id: str,
    fetched_at: str,
    status: str,
    payload_hash: str,
    payload: dict,
) -> None:
    conn.execute(
        "INSERT INTO snapshots (listing_id, fetched_at, status, payload_hash, payload_json) "
        "VALUES (?, ?, ?, ?, ?)",
        (listing_id, fetched_at, status, payload_hash,
         json.dumps(payload, ensure_ascii=False)),
    )


def insert_event(
    conn: sqlite3.Connection,
    listing_id: str,
    at: str,
    event_type: str,
    old_value: str | None,
    new_value: str | None,
) -> None:
    conn.execute(
        "INSERT INTO events (listing_id, at, event_type, old_value, new_value) "
        "VALUES (?, ?, ?, ?, ?)",
        (listing_id, at, event_type, old_value, new_value),
    )


def insert_price_history(
    conn: sqlite3.Connection,
    listing_id: str,
    price: float,
    price_raw: str | None,
    currency: str | None,
    recorded_at: str,
) -> None:
    conn.execute(
        "INSERT INTO price_history (listing_id, price, price_raw, currency, recorded_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (listing_id, price, price_raw, currency, recorded_at),
    )


# --- selects ----------------------------------------------------------------

def all_active_listings(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM listings WHERE is_removed = 0 ORDER BY last_seen_at DESC"
    ).fetchall()


def listing_by_id(conn: sqlite3.Connection, listing_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM listings WHERE id = ?", (listing_id,)
    ).fetchone()


def events_for(conn: sqlite3.Connection, listing_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM events WHERE listing_id = ? ORDER BY at DESC",
        (listing_id,),
    ).fetchall()


def snapshots_for(
    conn: sqlite3.Connection, listing_id: str, limit: int = 20
) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM snapshots WHERE listing_id = ? "
        "ORDER BY fetched_at DESC LIMIT ?",
        (listing_id, limit),
    ).fetchall()


def price_history_for(
    conn: sqlite3.Connection, listing_id: str
) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM price_history WHERE listing_id = ? ORDER BY recorded_at ASC",
        (listing_id,),
    ).fetchall()


def latest_snapshot_payload(
    conn: sqlite3.Connection, listing_id: str
) -> dict | None:
    row = conn.execute(
        "SELECT payload_json FROM snapshots WHERE listing_id = ? "
        "ORDER BY fetched_at DESC LIMIT 1",
        (listing_id,),
    ).fetchone()
    if not row:
        return None
    try:
        return json.loads(row["payload_json"])
    except (json.JSONDecodeError, TypeError):
        return None


def random_active_ids(
    conn: sqlite3.Connection, limit: int = 10
) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT id, url FROM listings WHERE is_removed = 0 "
        "ORDER BY RANDOM() LIMIT ?",
        (limit,),
    ).fetchall()
