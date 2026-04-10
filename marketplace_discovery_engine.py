"""
Discovery engine multi-estratégia.

Para um (keyword, region), aplica várias variações via `keyword_expander` e
combina os resultados num conjunto deduplicado por listing_id. Cada query
individual passa pelo cache TTL (`discovery_cache`) — múltiplos watchers
sobre o mesmo (keyword, region) só geram tráfego DDG uma vez por TTL.

Estratégias aplicadas em sequência:
    1. query direta:        keyword [+ region]
    2. variações:           keyword_expander.expand(keyword) [+ region cada]
    3. (futuro) categoria:  expansão por categoria detectada — placeholder

Output unificado:
    {
        "variations_tried": int,
        "queries_run": int,        # de fato chamou DDG
        "cache_hits": int,         # serviu do cache
        "total_unique_hits": int,
        "hits": [{url, item_id, title, source_query}, ...],
    }

Cada hit traz `source_query` informando qual variação o descobriu — útil
para entender quais variações estão pagando.

Uso CLI:
    python marketplace_discovery_engine.py iphone --region Araçatuba
    python marketplace_discovery_engine.py "playstation 5" --max-variations 6
"""

from __future__ import annotations

import argparse
import json

from discover_links import DEFAULT_UA, DuckDuckGoBackend, ITEM_RE
from discovery_cache import get as cache_get, put as cache_put
from keyword_expander import expand
from logging_setup import get_logger, kv

log = get_logger("discovery_engine")

DEFAULT_MAX_PAGES = 2
DEFAULT_MAX_VARIATIONS = 6


def _hits_to_dicts(hits) -> list[dict]:
    return [
        {"url": h.url, "item_id": h.item_id, "title": h.title}
        for h in hits
    ]


def _query_ddg(
    keyword: str, region: str | None, max_pages: int,
) -> list[dict]:
    """Wrapper sobre discover_links.discover() que retorna list[dict]."""
    from discover_links import discover as _discover
    backend = DuckDuckGoBackend()
    keywords = [keyword]
    if region:
        keywords.append(region)
    hits = _discover(keywords, backend, max_pages=max_pages)
    return _hits_to_dicts(hits)


def discover_for(
    keyword: str,
    region: str | None = None,
    max_pages: int = DEFAULT_MAX_PAGES,
    max_variations: int = DEFAULT_MAX_VARIATIONS,
    use_cache: bool = True,
) -> dict:
    """Roda múltiplas estratégias e devolve hits unificados."""
    variations = expand(keyword, max_variations=max_variations)
    if not variations:
        return {
            "variations_tried": 0, "queries_run": 0, "cache_hits": 0,
            "total_unique_hits": 0, "hits": [],
        }

    log.info(kv(event="discovery_start", keyword=keyword, region=region,
                variations=len(variations)))

    seen_ids: set[str] = set()
    all_hits: list[dict] = []
    queries_run = 0
    cache_hits = 0

    for variation in variations:
        cached = cache_get(variation, region) if use_cache else None
        if cached is not None:
            cache_hits += 1
            hits = cached
        else:
            try:
                hits = _query_ddg(variation, region, max_pages)
            except Exception as e:  # noqa: BLE001
                log.error(kv(event="ddg_error",
                             query=variation, error=type(e).__name__))
                continue
            queries_run += 1
            if use_cache:
                cache_put(variation, region, hits)

        for h in hits:
            iid = h.get("item_id")
            if not iid or iid in seen_ids:
                continue
            seen_ids.add(iid)
            all_hits.append({**h, "source_query": variation})

    result = {
        "variations_tried": len(variations),
        "queries_run": queries_run,
        "cache_hits": cache_hits,
        "total_unique_hits": len(all_hits),
        "hits": all_hits,
    }
    log.info(kv(event="discovery_done",
                keyword=keyword, region=region,
                **{k: v for k, v in result.items() if k != "hits"}))
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("keyword")
    ap.add_argument("--region")
    ap.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    ap.add_argument("--max-variations", type=int, default=DEFAULT_MAX_VARIATIONS)
    ap.add_argument("--no-cache", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    result = discover_for(
        keyword=args.keyword,
        region=args.region,
        max_pages=args.max_pages,
        max_variations=args.max_variations,
        use_cache=not args.no_cache,
    )

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    print(f"variations:    {result['variations_tried']}")
    print(f"queries_run:   {result['queries_run']}")
    print(f"cache_hits:    {result['cache_hits']}")
    print(f"unique_hits:   {result['total_unique_hits']}")
    print()
    for h in result["hits"][:30]:
        title = (h.get("title") or "")[:60]
        print(f"  [{h.get('source_query', '?'):20s}] {h['item_id']}  {title}")
    if len(result["hits"]) > 30:
        print(f"  ... ({len(result['hits']) - 30} mais)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
