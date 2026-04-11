"""
Módulo core de discovery. Função única e robusta que:
1. Busca no DDG com retry
2. Filtra URLs de marketplace
3. Valida no Facebook (extract)
4. Insere no banco

Usado por: /debug-discovery, watcher_engine, discover_links.

Não depende de nenhum outro módulo do projeto exceto db e extract_item.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass

from db import connect, init_db, insert_event, now_iso
from extract_item import Listing, extract
from logging_setup import get_logger, kv

log = get_logger("core_discovery")

ITEM_RE = re.compile(r"facebook\.com/marketplace/item/(\d+)")


@dataclass
class DiscoveryResult:
    queries_executed: list[str]
    urls_found: int
    validated: int
    active: int
    rejected: int
    inserted: int
    listings: list[dict]
    rejected_list: list[dict]
    errors: list[str]


def _search(query: str, max_results: int = 15) -> list[dict]:
    """Busca com fallback automático:
    1. Serper.dev (Google API) — se SERPER_API_KEY configurada
    2. DDG (ddgs lib) — fallback local

    Serper funciona de qualquer IP (ideal para servidores).
    DDG é bloqueado em datacenters (Render, AWS, etc).
    """
    import os
    serper_key = os.environ.get("SERPER_API_KEY")
    if serper_key:
        return _serper_search(query, serper_key, max_results)
    return _ddg_search(query, max_results)


def _serper_search(query: str, api_key: str, max_results: int = 15) -> list[dict]:
    """Google via Serper.dev — funciona de qualquer IP, 2500 buscas/mês grátis."""
    import requests as _req
    try:
        resp = _req.post(
            "https://google.serper.dev/search",
            json={"q": query, "num": min(max_results, 20)},
            headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        results = []
        for item in data.get("organic", []):
            results.append({
                "href": item.get("link", ""),
                "title": item.get("title", ""),
            })
        log.info(kv(event="serper_search", query=query[:50],
                     results=len(results)))
        return results
    except Exception as e:
        log.error(kv(event="serper_error", error=str(e)[:100]))
        return []


def _ddg_search(query: str, max_results: int = 15) -> list[dict]:
    """Fallback: DDG via lib ddgs. Bloqueado em IPs de datacenter."""
    try:
        from ddgs import DDGS
    except ImportError:
        log.warning(kv(event="ddgs_not_installed"))
        return []

    for attempt in range(3):
        try:
            return DDGS().text(query, max_results=max_results)
        except Exception as e:
            log.warning(kv(event="ddg_retry", attempt=attempt,
                           error=str(e)[:80]))
            if attempt < 2:
                time.sleep(2)
    return []


def discover_and_validate(
    keyword: str,
    region: str | None = None,
    max_ddg_results: int = 15,
    max_validate: int = 10,
    extra_queries: list[str] | None = None,
    persist: bool = True,
) -> DiscoveryResult:
    """Pipeline completo: DDG → filtro marketplace → extract FB → persist.

    Args:
        keyword: palavra-chave principal
        region: região opcional (adicionada à query)
        max_ddg_results: resultados por query DDG
        max_validate: máximo de URLs a validar no Facebook
        extra_queries: queries adicionais além da principal
        persist: se True, insere no banco
    """
    init_db()
    errors: list[str] = []

    # 1. Montar queries
    # Serper (Google): site: e inurl: funcionam → retornam itens individuais
    # DDG: site: é bloqueado → usa queries naturais como fallback
    import os
    region_suffix = f" {region}" if region else ""
    if os.environ.get("SERPER_API_KEY"):
        # Google via Serper: operadores avançados funcionam
        queries = [
            f"site:facebook.com/marketplace/item {keyword}{region_suffix}",
            f"inurl:marketplace/item {keyword}{region_suffix}",
        ]
    else:
        # DDG fallback: queries naturais (site: não funciona)
        queries = [
            f"facebook marketplace {keyword} vendo{region_suffix}",
            f"facebook marketplace {keyword} usado{region_suffix}",
            f"facebook.com/marketplace {keyword}{region_suffix}",
        ]
    if extra_queries:
        queries.extend(extra_queries)

    # 2. Executar DDG
    seen_ids: set[str] = set()
    raw_urls: list[dict] = []

    for q in queries:
        results = _search(q, max_results=max_ddg_results)
        for r in results:
            href = r.get("href") or r.get("url") or ""
            m = ITEM_RE.search(href)
            if m and m.group(1) not in seen_ids:
                seen_ids.add(m.group(1))
                raw_urls.append({
                    "item_id": m.group(1),
                    "url": href,
                    "title_ddg": r.get("title", ""),
                })

    # 3. Validar no Facebook
    to_validate = raw_urls[:max_validate]
    active_listings: list[dict] = []
    rejected_list: list[dict] = []

    for i, u in enumerate(to_validate):
        try:
            listing = extract(u["item_id"])
            if listing.status == "ok" and listing.title:
                active_listings.append({
                    "item_id": u["item_id"],
                    "url": u["url"],
                    "title": listing.title,
                    "price": listing.price_formatted or listing.price_amount or "",
                    "currency": listing.price_currency or "",
                    "location": listing.location_text or "",
                    "status": "ok",
                    "_listing": listing,
                })
            else:
                rejected_list.append({
                    "item_id": u["item_id"],
                    "reason": listing.status,
                    "title_ddg": u["title_ddg"][:60],
                })
        except Exception as e:
            errors.append(f"extract {u['item_id']}: {e}")
            rejected_list.append({
                "item_id": u["item_id"],
                "reason": f"erro: {type(e).__name__}",
                "title_ddg": u["title_ddg"][:60],
            })
        if i < len(to_validate) - 1:
            time.sleep(2)

    # 4. Persistir no banco
    inserted = 0
    if persist and active_listings:
        now = now_iso()
        with connect() as conn:
            for a in active_listings:
                existing = conn.execute(
                    "SELECT id FROM listings WHERE id = ?", (a["item_id"],)
                ).fetchone()
                if not existing:
                    listing = a["_listing"]
                    conn.execute(
                        """INSERT INTO listings
                          (id, url, source, first_seen_at, last_seen_at,
                           last_status, current_title, current_price,
                           current_currency, current_location)
                        VALUES (?, ?, 'discovery', ?, ?, 'ok', ?, ?, ?, ?)""",
                        (a["item_id"], a["url"], now, now,
                         a["title"], a["price"], a["currency"], a["location"]),
                    )
                    insert_event(conn, a["item_id"], now, "first_seen",
                                 None, a["title"])
                    inserted += 1

    # Limpar referências internas antes de retornar
    for a in active_listings:
        a.pop("_listing", None)

    log.info(kv(event="discovery_complete", keyword=keyword, region=region,
                urls=len(raw_urls), active=len(active_listings),
                rejected=len(rejected_list), inserted=inserted))

    return DiscoveryResult(
        queries_executed=queries,
        urls_found=len(raw_urls),
        validated=len(to_validate),
        active=len(active_listings),
        rejected=len(rejected_list),
        inserted=inserted,
        listings=active_listings,
        rejected_list=rejected_list,
        errors=errors,
    )
