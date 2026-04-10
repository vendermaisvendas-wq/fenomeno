"""
Insere dados simulados no banco para testar o sistema sem precisar de
internet / discovery real.

Cria:
  - 10 listings simulados (veículos + eletrônicos + móveis)
  - 2 watchers (iphone + hilux)
  - Associa listings aos watchers via watcher_results
  - Cria eventos de preço e alerta

Depois de rodar este script:
  - O dashboard mostra anúncios na página principal
  - A página /watchers mostra monitoramentos com matches
  - A página /stats mostra estatísticas

Uso:
    python seed_test_data.py
    python seed_test_data.py --clean     # limpa tudo antes de inserir
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from db import connect, init_db, insert_event, insert_price_history, insert_snapshot, now_iso


SAMPLE_LISTINGS = [
    {
        "id": "SIM001", "title": "iPhone 13 128GB Preto Seminovo",
        "price": "3200", "currency": "BRL", "location": "Araçatuba, SP",
        "category": "electronics",
    },
    {
        "id": "SIM002", "title": "iPhone 12 64GB Branco",
        "price": "2500", "currency": "BRL", "location": "Araçatuba, SP",
        "category": "electronics",
    },
    {
        "id": "SIM003", "title": "iPhone 11 128GB Bateria 90%",
        "price": "2100", "currency": "BRL", "location": "São Paulo, SP",
        "category": "electronics",
    },
    {
        "id": "SIM004", "title": "Samsung Galaxy S22 Ultra 256GB",
        "price": "3800", "currency": "BRL", "location": "São Paulo, SP",
        "category": "electronics",
    },
    {
        "id": "SIM005", "title": "Toyota Hilux SRV 2020 Diesel 4x4",
        "price": "185000", "currency": "BRL", "location": "Araçatuba, SP",
        "category": "vehicles",
    },
    {
        "id": "SIM006", "title": "Toyota Hilux SR 2019 Diesel",
        "price": "165000", "currency": "BRL", "location": "Birigui, SP",
        "category": "vehicles",
    },
    {
        "id": "SIM007", "title": "Honda Civic EXL 2020 Flex Automático",
        "price": "115000", "currency": "BRL", "location": "Araçatuba, SP",
        "category": "vehicles",
    },
    {
        "id": "SIM008", "title": "Sofá de Canto 3 Lugares Retrátil",
        "price": "1800", "currency": "BRL", "location": "Araçatuba, SP",
        "category": "furniture",
    },
    {
        "id": "SIM009", "title": "Notebook Dell Inspiron i7 16GB RAM",
        "price": "4200", "currency": "BRL", "location": "São Paulo, SP",
        "category": "electronics",
    },
    {
        "id": "SIM010", "title": "PlayStation 5 com 2 Controles",
        "price": "3500", "currency": "BRL", "location": "Araçatuba, SP",
        "category": "electronics",
    },
]


def clean_db():
    with connect() as conn:
        conn.execute("DELETE FROM watcher_results WHERE listing_id LIKE 'SIM%'")
        conn.execute("DELETE FROM events WHERE listing_id LIKE 'SIM%'")
        conn.execute("DELETE FROM snapshots WHERE listing_id LIKE 'SIM%'")
        conn.execute("DELETE FROM price_history WHERE listing_id LIKE 'SIM%'")
        conn.execute("DELETE FROM listings WHERE id LIKE 'SIM%'")
        conn.execute("DELETE FROM watchers WHERE keyword IN ('iphone', 'hilux')")
    print("[limpo] dados simulados removidos")


def seed():
    init_db()
    now = datetime.now(timezone.utc)
    now_str = now.isoformat(timespec="seconds")

    with connect() as conn:
        # Inserir listings
        for i, item in enumerate(SAMPLE_LISTINGS):
            age = timedelta(hours=i * 3)
            first_seen = (now - age).isoformat(timespec="seconds")
            conn.execute(
                """
                INSERT OR IGNORE INTO listings
                  (id, url, source, first_seen_at, last_seen_at, last_status,
                   current_title, current_price, current_currency,
                   current_location, category, city, state)
                VALUES (?, ?, 'seed_test', ?, ?, 'ok', ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item["id"],
                    f"https://www.facebook.com/marketplace/item/{item['id']}/",
                    first_seen,
                    now_str,
                    item["title"],
                    item["price"],
                    item["currency"],
                    item["location"],
                    item["category"],
                    item["location"].split(",")[0].strip(),
                    item["location"].split(",")[-1].strip() if "," in item["location"] else None,
                ),
            )
            # Snapshot
            insert_snapshot(conn, item["id"], now_str, "ok", f"hash_{item['id']}",
                            {"title": item["title"], "price": item["price"],
                             "description": f"Anúncio simulado: {item['title']}",
                             "image_urls": []})
            # Evento first_seen
            insert_event(conn, item["id"], first_seen, "first_seen", None, item["title"])
            # Price history
            from price_normalizer import parse as pp
            price_float = pp(item["price"])
            if price_float:
                insert_price_history(conn, item["id"], price_float,
                                     item["price"], item["currency"], first_seen)

        print(f"[seed] {len(SAMPLE_LISTINGS)} anúncios simulados inseridos")

        # Criar watchers
        for kw, region in [("iphone", "Araçatuba"), ("hilux", "Araçatuba")]:
            conn.execute(
                """
                INSERT OR IGNORE INTO watchers
                  (keyword, region, is_active, priority, created_at)
                VALUES (?, ?, 1, 1, ?)
                """,
                (kw, region, now_str),
            )
        watchers = conn.execute(
            "SELECT watch_id, keyword FROM watchers WHERE keyword IN ('iphone', 'hilux')"
        ).fetchall()
        print(f"[seed] {len(watchers)} monitoramentos criados")

        # Associar listings aos watchers
        matched = 0
        for w in watchers:
            kw = w["keyword"].lower()
            for item in SAMPLE_LISTINGS:
                if kw in item["title"].lower():
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO watcher_results
                          (watch_id, listing_id, first_seen, is_initial_backfill)
                        VALUES (?, ?, ?, 1)
                        """,
                        (w["watch_id"], item["id"], now_str),
                    )
                    matched += 1
        print(f"[seed] {matched} associacoes watcher<->listing")

    print()
    print("Dados de teste inseridos com sucesso!")
    print("Acesse o dashboard para ver os resultados:")
    print("  http://localhost:8000")
    print("  http://localhost:8000/watchers")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--clean", action="store_true",
                    help="limpa dados simulados antes de inserir")
    args = ap.parse_args()

    if args.clean:
        init_db()
        clean_db()
    seed()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
