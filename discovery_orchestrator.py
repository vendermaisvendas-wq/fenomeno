"""
Orquestrador de discovery multi-estratégia.

Estratégias implementadas (as que funcionam de verdade num servidor):

  1. serper_site    — Google via Serper com site:facebook.com/marketplace/item
  2. serper_inurl   — Google via Serper com inurl:marketplace/item
  3. serper_natural — Google via Serper com queries naturais
  4. ddg_natural    — DuckDuckGo com queries naturais (fallback, bloqueado em DC)
  5. keyword_variations — multiplica cada estratégia com variações automáticas

Cada estratégia retorna URLs. O orquestrador:
  - roda todas em sequência (com fallback se uma falhar)
  - deduplica por listing_id
  - valida no Facebook (extract_item)
  - persiste os ativos no banco

Uso:
    python discovery_orchestrator.py iphone
    python discovery_orchestrator.py iphone --region Araçatuba --validate 8
    python discovery_orchestrator.py --strategies   # lista estratégias
"""

from __future__ import annotations

import argparse
import os
import re
import time
from dataclasses import dataclass, field

from db import connect, init_db, insert_event, now_iso
from extract_item import extract
from logging_setup import get_logger, kv

log = get_logger("orchestrator")

ITEM_RE = re.compile(r"facebook\.com/marketplace/item/(\d+)")


@dataclass
class StrategyResult:
    name: str
    urls_found: int
    item_ids: list[str]
    error: str | None = None


@dataclass
class OrchestratorResult:
    strategies_run: int
    strategies_ok: int
    strategies_failed: int
    total_urls: int
    unique_ids: int
    validated: int
    active: int
    rejected: int
    inserted: int
    per_strategy: list[dict]
    listings: list[dict] = field(default_factory=list)
    rejected_list: list[dict] = field(default_factory=list)


# === ESTRATÉGIAS ===

def _serper_raw(query: str, api_key: str, num: int = 10) -> list[dict]:
    """Chama Serper e retorna resultados brutos."""
    import requests
    try:
        resp = requests.post(
            "https://google.serper.dev/search",
            json={"q": query, "num": num},
            headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("organic", [])
    except Exception as e:
        log.warning(kv(event="serper_error", query=query[:50], error=str(e)[:80]))
        return []


def _extract_marketplace_ids(results: list[dict]) -> list[tuple[str, str, str]]:
    """Extrai (item_id, url, title) dos resultados de busca."""
    out = []
    for r in results:
        link = r.get("link") or r.get("href") or ""
        m = ITEM_RE.search(link)
        if m:
            out.append((m.group(1), link, r.get("title", "")))
    return out


def strategy_serper_site(keyword: str, region: str | None, api_key: str) -> StrategyResult:
    """Google com site:facebook.com/marketplace/item."""
    q = f"site:facebook.com/marketplace/item {keyword}"
    if region:
        q += f" {region}"
    results = _serper_raw(q, api_key, num=15)
    items = _extract_marketplace_ids(results)
    return StrategyResult(
        name="serper_site",
        urls_found=len(items),
        item_ids=[i[0] for i in items],
    )


def strategy_serper_inurl(keyword: str, region: str | None, api_key: str) -> StrategyResult:
    """Google com inurl:marketplace/item."""
    q = f"inurl:marketplace/item {keyword}"
    if region:
        q += f" {region}"
    results = _serper_raw(q, api_key, num=15)
    items = _extract_marketplace_ids(results)
    return StrategyResult(
        name="serper_inurl",
        urls_found=len(items),
        item_ids=[i[0] for i in items],
    )


def strategy_serper_natural(keyword: str, region: str | None, api_key: str) -> StrategyResult:
    """Google com queries naturais (vendo, usado)."""
    all_items = []
    for suffix in ["vendo", "usado", "barato"]:
        q = f"facebook marketplace {keyword} {suffix}"
        if region:
            q += f" {region}"
        results = _serper_raw(q, api_key, num=10)
        all_items.extend(_extract_marketplace_ids(results))
    return StrategyResult(
        name="serper_natural",
        urls_found=len(all_items),
        item_ids=[i[0] for i in all_items],
    )


def strategy_serper_variations(keyword: str, region: str | None, api_key: str) -> StrategyResult:
    """Google com variações automáticas de keyword."""
    try:
        from keyword_expander import expand
        variations = expand(keyword, max_variations=5)
    except Exception:
        variations = [keyword]

    all_items = []
    for v in variations[1:4]:  # pula o original (já coberto), pega 3 variações
        q = f"site:facebook.com/marketplace/item {v}"
        if region:
            q += f" {region}"
        results = _serper_raw(q, api_key, num=10)
        all_items.extend(_extract_marketplace_ids(results))
    return StrategyResult(
        name="serper_variations",
        urls_found=len(all_items),
        item_ids=[i[0] for i in all_items],
    )


def strategy_ddg(keyword: str, region: str | None) -> StrategyResult:
    """DuckDuckGo (fallback). Bloqueado em datacenters."""
    try:
        from ddgs import DDGS
    except ImportError:
        return StrategyResult(name="ddg", urls_found=0, item_ids=[],
                              error="ddgs não instalado")
    q = f"facebook marketplace {keyword} vendo"
    if region:
        q += f" {region}"
    try:
        results = DDGS().text(q, max_results=15)
        items = []
        for r in results:
            href = r.get("href", "")
            m = ITEM_RE.search(href)
            if m:
                items.append(m.group(1))
        return StrategyResult(name="ddg", urls_found=len(items), item_ids=items)
    except Exception as e:
        return StrategyResult(name="ddg", urls_found=0, item_ids=[],
                              error=str(e)[:80])


# === ORQUESTRADOR ===

def run_all_strategies(
    keyword: str,
    region: str | None = None,
) -> tuple[list[StrategyResult], set[str]]:
    """Roda todas as estratégias, deduplica IDs."""
    api_key = os.environ.get("SERPER_API_KEY")
    results: list[StrategyResult] = []
    all_ids: set[str] = set()

    if api_key:
        for fn in [strategy_serper_site, strategy_serper_inurl,
                    strategy_serper_natural, strategy_serper_variations]:
            try:
                r = fn(keyword, region, api_key)
                results.append(r)
                all_ids.update(r.item_ids)
            except Exception as e:
                results.append(StrategyResult(
                    name=fn.__name__.replace("strategy_", ""),
                    urls_found=0, item_ids=[], error=str(e)[:80],
                ))
            time.sleep(0.5)
    else:
        log.warning(kv(event="no_serper_key", fallback="ddg"))

    # DDG como fallback (pode falhar em datacenter, tudo bem)
    try:
        r = strategy_ddg(keyword, region)
        results.append(r)
        all_ids.update(r.item_ids)
    except Exception as e:
        results.append(StrategyResult(
            name="ddg", urls_found=0, item_ids=[], error=str(e)[:80],
        ))

    return results, all_ids


def discover_validate_persist(
    keyword: str,
    region: str | None = None,
    max_validate: int = 10,
    persist: bool = True,
) -> OrchestratorResult:
    """Pipeline completo: estratégias → dedup → validate FB → persist."""
    init_db()

    # 1. Estratégias
    strategy_results, all_ids = run_all_strategies(keyword, region)
    unique_ids = list(all_ids)[:max_validate * 2]  # margem para rejeições

    strategies_ok = sum(1 for r in strategy_results if r.error is None)
    strategies_failed = sum(1 for r in strategy_results if r.error is not None)
    total_urls = sum(r.urls_found for r in strategy_results)

    # 2. Validar no Facebook
    validated_listings: list[dict] = []
    rejected_list: list[dict] = []
    to_validate = unique_ids[:max_validate]

    for i, item_id in enumerate(to_validate):
        url = f"https://www.facebook.com/marketplace/item/{item_id}/"
        try:
            listing = extract(item_id)
            if listing.status == "ok" and listing.title:
                validated_listings.append({
                    "item_id": item_id,
                    "url": url,
                    "title": listing.title,
                    "price": listing.price_formatted or listing.price_amount or "",
                    "currency": listing.price_currency or "",
                    "location": listing.location_text or "",
                    "_listing": listing,
                })
            else:
                rejected_list.append({
                    "item_id": item_id,
                    "reason": listing.status,
                })
        except Exception as e:
            rejected_list.append({"item_id": item_id, "reason": str(e)[:50]})
        if i < len(to_validate) - 1:
            time.sleep(2)

    # 3. Persistir
    inserted = 0
    if persist and validated_listings:
        now = now_iso()
        with connect() as conn:
            for v in validated_listings:
                existing = conn.execute(
                    "SELECT id FROM listings WHERE id = ?", (v["item_id"],)
                ).fetchone()
                if not existing:
                    listing = v["_listing"]
                    conn.execute(
                        """INSERT INTO listings
                          (id, url, source, first_seen_at, last_seen_at,
                           last_status, current_title, current_price,
                           current_currency, current_location)
                        VALUES (?, ?, 'discovery', ?, ?, 'ok', ?, ?, ?, ?)""",
                        (v["item_id"], v["url"], now, now,
                         v["title"], v["price"], v["currency"], v["location"]),
                    )
                    insert_event(conn, v["item_id"], now, "first_seen",
                                 None, v["title"])
                    inserted += 1

    for v in validated_listings:
        v.pop("_listing", None)

    return OrchestratorResult(
        strategies_run=len(strategy_results),
        strategies_ok=strategies_ok,
        strategies_failed=strategies_failed,
        total_urls=total_urls,
        unique_ids=len(all_ids),
        validated=len(to_validate),
        active=len(validated_listings),
        rejected=len(rejected_list),
        inserted=inserted,
        per_strategy=[
            {"name": r.name, "urls": r.urls_found, "error": r.error}
            for r in strategy_results
        ],
        listings=validated_listings,
        rejected_list=rejected_list,
    )


# === CLI ===

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("keyword", nargs="?")
    ap.add_argument("--region")
    ap.add_argument("--validate", type=int, default=8)
    ap.add_argument("--strategies", action="store_true",
                    help="lista estratégias disponíveis")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.strategies:
        api_key = os.environ.get("SERPER_API_KEY")
        print("Estratégias disponíveis:")
        print(f"  serper_site       {'OK' if api_key else 'precisa SERPER_API_KEY'}")
        print(f"  serper_inurl      {'OK' if api_key else 'precisa SERPER_API_KEY'}")
        print(f"  serper_natural    {'OK' if api_key else 'precisa SERPER_API_KEY'}")
        print(f"  serper_variations {'OK' if api_key else 'precisa SERPER_API_KEY'}")
        print(f"  ddg               OK (bloqueado em datacenters)")
        return 0

    if not args.keyword:
        ap.error("forneça a keyword")

    result = discover_validate_persist(
        keyword=args.keyword,
        region=args.region,
        max_validate=args.validate,
        persist=not args.dry_run,
    )

    print(f"\n{'='*50}")
    print(f"  DISCOVERY MULTI-ESTRATÉGIA: {args.keyword}")
    print(f"{'='*50}")
    print(f"\nEstratégias: {result.strategies_run} ({result.strategies_ok} OK, "
          f"{result.strategies_failed} falharam)")
    for s in result.per_strategy:
        status = f"{s['urls']} URLs" if s["error"] is None else f"ERRO: {s['error'][:50]}"
        print(f"  {s['name']:20s} {status}")
    print(f"\nIDs únicos: {result.unique_ids}")
    print(f"Validados no FB: {result.validated}")
    print(f"Ativos: {result.active}")
    print(f"Rejeitados: {result.rejected}")
    print(f"Inseridos no banco: {result.inserted}")
    if result.listings:
        print(f"\nAnúncios encontrados:")
        for l in result.listings:
            print(f"  {l['item_id']}  {l['title'][:50]}  {l['price']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
