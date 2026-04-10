"""
Descobre URLs públicas de itens do Marketplace via indexação em buscadores.

Backend padrão: DuckDuckGo HTML (`html.duckduckgo.com/html/`). DDG é mais
tolerante a tráfego automatizado que o Google e não exige chave. Para escala
real ou para quem quer cobertura Google, o caminho correto é uma API paga
(SerpAPI, Serper, Oxylabs SERP) — deixamos a interface `SearchBackend`
plugável pra isso.

O script roda uma query `site:facebook.com/marketplace/item <keywords>`,
filtra URLs que batem no padrão de item, extrai o ID e insere no banco
SQLite (via db.py) com `source = "discover:<backend>"`. Já existentes são
ignorados — só novos IDs retornam como "descobertos".

Uso:
    python discover_links.py moto
    python discover_links.py iphone 13
    python discover_links.py --max-pages 3 carro
"""

from __future__ import annotations

import argparse
import re
import sys
import time
import urllib.parse
from dataclasses import dataclass
from typing import Iterator, Protocol

import requests
from bs4 import BeautifulSoup

from db import connect, discover_insert, init_db

DEFAULT_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Firefox/125.0"
)
ITEM_RE = re.compile(r"facebook\.com/marketplace/item/(\d+)")


@dataclass
class Hit:
    url: str
    item_id: str
    title: str | None
    backend: str


class SearchBackend(Protocol):
    name: str
    def search(self, query: str, max_pages: int) -> Iterator[tuple[str, str]]: ...


class DuckDuckGoBackend:
    """DDG HTML endpoint. Retorna (title, target_url) por resultado."""
    name = "ddg"
    base = "https://html.duckduckgo.com/html/"

    def __init__(self, delay_range: tuple[float, float] = (5.0, 9.0)) -> None:
        self.delay_range = delay_range

    def search(self, query: str, max_pages: int) -> Iterator[tuple[str, str]]:
        for page in range(max_pages):
            params = {"q": query}
            if page:
                params["s"] = str(page * 30)
            try:
                r = requests.get(
                    self.base,
                    params=params,
                    headers={"User-Agent": DEFAULT_UA, "Accept-Language": "pt-BR,pt;q=0.9"},
                    timeout=20,
                )
            except requests.RequestException as e:
                print(f"[ddg] request error: {e}", file=sys.stderr)
                return
            if r.status_code != 200:
                print(f"[ddg] http {r.status_code}", file=sys.stderr)
                return
            soup = BeautifulSoup(r.text, "html.parser")
            results = soup.select("a.result__a")
            if not results:
                # DDG pode ter retornado anti-bot page
                return
            for a in results:
                href = a.get("href", "")
                title = a.get_text(strip=True) or None
                resolved = _resolve_ddg_href(href)
                if resolved:
                    yield title or "", resolved
            if page < max_pages - 1:
                time.sleep(_jitter(*self.delay_range))


def _jitter(lo: float, hi: float) -> float:
    import random
    return random.uniform(lo, hi)


def _resolve_ddg_href(href: str) -> str | None:
    """DDG embrulha links em /l/?uddg=<url>. Desembrulha."""
    if not href:
        return None
    parsed = urllib.parse.urlparse(href)
    if parsed.path.startswith("/l/"):
        qs = urllib.parse.parse_qs(parsed.query)
        if "uddg" in qs:
            return urllib.parse.unquote(qs["uddg"][0])
        return None
    if parsed.scheme in ("http", "https"):
        return href
    return None


def build_query(keywords: list[str]) -> str:
    phrase = " ".join(keywords).strip()
    return f"site:facebook.com/marketplace/item {phrase}".strip()


def discover(
    keywords: list[str],
    backend: SearchBackend,
    max_pages: int = 2,
) -> list[Hit]:
    query = build_query(keywords)
    print(f"[discover] query: {query!r} (backend={backend.name}, pages={max_pages})")

    hits: list[Hit] = []
    seen_ids: set[str] = set()
    for title, url in backend.search(query, max_pages):
        m = ITEM_RE.search(url)
        if not m:
            continue
        item_id = m.group(1)
        if item_id in seen_ids:
            continue
        seen_ids.add(item_id)
        canonical = f"https://www.facebook.com/marketplace/item/{item_id}/"
        hits.append(Hit(url=canonical, item_id=item_id, title=title, backend=backend.name))
    return hits


def persist(hits: list[Hit]) -> tuple[int, int]:
    """Insere novos hits no banco. Retorna (inseridos, já existentes)."""
    init_db()
    inserted = 0
    skipped = 0
    with connect() as conn:
        for h in hits:
            if discover_insert(conn, h.item_id, h.url, f"discover:{h.backend}"):
                inserted += 1
            else:
                skipped += 1
    return inserted, skipped


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("keywords", nargs="+", help="palavras-chave da busca")
    ap.add_argument("--max-pages", type=int, default=2,
                    help="páginas de resultados a percorrer (default: 2)")
    ap.add_argument("--dry-run", action="store_true",
                    help="não grava no banco, só lista")
    args = ap.parse_args()

    backend = DuckDuckGoBackend()
    hits = discover(args.keywords, backend, max_pages=args.max_pages)

    print(f"[discover] {len(hits)} URLs de item candidatas")
    for h in hits:
        title = (h.title or "(sem título)")[:80]
        print(f"  {h.item_id}  {title}")

    if not hits:
        print("[discover] nada encontrado — buscador pode ter devolvido anti-bot.")
        return 1

    if args.dry_run:
        return 0

    inserted, skipped = persist(hits)
    print(f"[discover] inseridos: {inserted}, já existiam: {skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
