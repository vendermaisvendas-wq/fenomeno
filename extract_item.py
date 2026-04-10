"""
Extrator de dados públicos de um item do Facebook Marketplace.

Parsing em 4 camadas, cada campo registra a camada que o encontrou:
  L1 — JSON-LD           (schema.org embutido — mais estável quando existe)
  L2 — OpenGraph         (meta tags populadas pelo SSR)
  L3 — Relay regex       (chaves internas do Relay no HTML bruto)
  L4 — DOM fallback      (BeautifulSoup em h1/texto visível)

Expõe duas APIs: `extract(id_or_url)` (sync, requests) e
`extract_async(id_or_url, client)` (httpx.AsyncClient). Ambas chamam o mesmo
`parse_html()` puro.

Uso:
    python extract_item.py 2015275022700246
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Iterator

import httpx
import requests
from bs4 import BeautifulSoup

from db import now_iso
from html_cache import save_html, save_listing_json
from logging_setup import get_logger, kv

log = get_logger("extract")

CONTACT = "marketplace-audit@example.invalid"
USER_AGENT = (
    f"FBMarketplaceAudit/0.2 (+mailto:{CONTACT}) "
    "Mozilla/5.0 (compatible; research-only; rate-limited)"
)
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}
TIMEOUT = 20
ITEM_URL = "https://www.facebook.com/marketplace/item/{id}/"


@dataclass
class Listing:
    id: str
    url: str
    fetched_at: str
    status: str = "ok"
    title: str | None = None
    price_amount: str | None = None
    price_currency: str | None = None
    price_formatted: str | None = None
    description: str | None = None
    location_text: str | None = None
    category: str | None = None
    image_urls: list[str] = field(default_factory=list)
    primary_image_url: str | None = None
    seller_name: str | None = None
    creation_time: int | None = None
    raw_og: dict[str, str] = field(default_factory=dict)
    field_sources: dict[str, str] = field(default_factory=dict)
    extraction_method: str = ""


# --- utilidades -------------------------------------------------------------

def normalize_target(arg: str) -> tuple[str, str]:
    m = re.search(r"/marketplace/item/(\d+)", arg)
    if m:
        return m.group(1), ITEM_URL.format(id=m.group(1))
    if arg.isdigit():
        return arg, ITEM_URL.format(id=arg)
    raise ValueError(f"Não consegui interpretar: {arg!r}")


def detect_login_wall(html: str, final_url: str) -> bool:
    if "/login" in final_url:
        return True
    lowered = html[:20000].lower()
    return any(m in lowered for m in ('id="login_form"', "log in to facebook"))


def detect_not_found(html: str) -> bool:
    markers = (
        "this content isn't available",
        "isn't available right now",
        "este conteúdo não está disponível",
        "conteúdo não está disponível",
    )
    lowered = html[:50000].lower()
    return any(m in lowered for m in markers)


def _unescape(s: str) -> str:
    try:
        return json.loads(f'"{s}"')
    except json.JSONDecodeError:
        return s


def _mark(listing: Listing, attr: str, value: Any, source: str) -> bool:
    """Seta `attr` apenas se ainda estiver vazio. Registra a camada de origem."""
    if value in (None, "", []):
        return False
    if getattr(listing, attr):
        return False
    setattr(listing, attr, value)
    listing.field_sources[attr] = source
    return True


# --- L1: JSON-LD ------------------------------------------------------------

def apply_jsonld(listing: Listing, soup: BeautifulSoup) -> bool:
    """Procura script type=application/ld+json. FB raramente fornece, mas é a
    camada mais estável quando existe (schema.org/Product)."""
    got = False
    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or script.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for obj in data if isinstance(data, list) else [data]:
            if not isinstance(obj, dict):
                continue
            t = obj.get("@type")
            if t not in ("Product", "Offer", "IndividualProduct"):
                continue
            got |= _mark(listing, "title", obj.get("name"), "jsonld")
            got |= _mark(listing, "description", obj.get("description"), "jsonld")
            offers = obj.get("offers") or {}
            if isinstance(offers, list) and offers:
                offers = offers[0]
            if isinstance(offers, dict):
                got |= _mark(listing, "price_amount", str(offers.get("price") or "") or None, "jsonld")
                got |= _mark(listing, "price_currency", offers.get("priceCurrency"), "jsonld")
            img = obj.get("image")
            if isinstance(img, str):
                got |= _mark(listing, "primary_image_url", img, "jsonld")
                if img not in listing.image_urls:
                    listing.image_urls.append(img)
            elif isinstance(img, list):
                for u in img:
                    if isinstance(u, str) and u not in listing.image_urls:
                        listing.image_urls.append(u)
                        got = True
    return got


# --- L2: OpenGraph ----------------------------------------------------------

def parse_opengraph(soup: BeautifulSoup) -> dict[str, str]:
    out: dict[str, str] = {}
    for tag in soup.find_all("meta"):
        prop = tag.get("property") or tag.get("name")
        content = tag.get("content")
        if prop and content:
            out[prop] = content
    return out


def apply_og(listing: Listing, og: dict[str, str]) -> bool:
    if not og:
        return False
    listing.raw_og = og
    got = False
    if "og:title" in og:
        title = re.sub(r"\s*\|\s*Facebook( Marketplace)?$", "", og["og:title"]).strip()
        got |= _mark(listing, "title", title or None, "og")
    got |= _mark(listing, "description", og.get("og:description"), "og")
    if "og:image" in og:
        got |= _mark(listing, "primary_image_url", og["og:image"], "og")
        if og["og:image"] not in listing.image_urls:
            listing.image_urls.insert(0, og["og:image"])
    got |= _mark(listing, "price_amount", og.get("product:price:amount"), "og")
    got |= _mark(listing, "price_currency", og.get("product:price:currency"), "og")
    return got


# --- L3: Relay regex --------------------------------------------------------

RE_TITLE_KEY = re.compile(r'"marketplace_listing_title"\s*:\s*"((?:[^"\\]|\\.)*)"')
RE_PRICE_AMOUNT = re.compile(r'"listing_price"\s*:\s*\{[^}]*?"amount"\s*:\s*"([^"]+)"')
RE_PRICE_FORMATTED = re.compile(
    r'"listing_price"\s*:\s*\{[^}]*?"formatted_amount"\s*:\s*"((?:[^"\\]|\\.)*)"'
)
RE_PRICE_CURRENCY = re.compile(
    r'"listing_price"\s*:\s*\{[^}]*?"currency"\s*:\s*"([A-Z]{3})"'
)
RE_DESCRIPTION = re.compile(
    r'"redacted_description"\s*:\s*\{\s*"text"\s*:\s*"((?:[^"\\]|\\.)*)"'
)
RE_LOCATION_TEXT = re.compile(
    r'"location_text"\s*:\s*\{\s*"text"\s*:\s*"((?:[^"\\]|\\.)*)"'
)
RE_CREATION_TIME = re.compile(r'"creation_time"\s*:\s*(\d{9,11})')
RE_PHOTO_URI = re.compile(
    r'"(?:uri|image)"\s*:\s*"(https:\\?/\\?/scontent[^"]*?\.fbcdn\.net\\?/[^"]+)"'
)
RE_SELLER_NAME = re.compile(
    r'"marketplace_listing_seller"\s*:\s*\{[^}]*?"name"\s*:\s*"((?:[^"\\]|\\.)*)"'
)
RE_CATEGORY = re.compile(
    r'"marketplace_listing_category_name"\s*:\s*"((?:[^"\\]|\\.)*)"'
)


def apply_relay_regex(listing: Listing, html: str) -> bool:
    got = False

    def try_set(attr: str, pattern: re.Pattern[str], transform=_unescape):
        nonlocal got
        m = pattern.search(html)
        if m:
            got |= _mark(listing, attr, transform(m.group(1)), "relay")

    try_set("title", RE_TITLE_KEY)
    try_set("price_amount", RE_PRICE_AMOUNT, lambda s: s)
    try_set("price_formatted", RE_PRICE_FORMATTED)
    try_set("price_currency", RE_PRICE_CURRENCY, lambda s: s)
    try_set("description", RE_DESCRIPTION)
    try_set("location_text", RE_LOCATION_TEXT)
    try_set("category", RE_CATEGORY)
    try_set("seller_name", RE_SELLER_NAME)

    if listing.creation_time is None:
        m = RE_CREATION_TIME.search(html)
        if m:
            listing.creation_time = int(m.group(1))
            listing.field_sources["creation_time"] = "relay"
            got = True

    seen = set(listing.image_urls)
    for m in RE_PHOTO_URI.finditer(html):
        uri = m.group(1).replace("\\/", "/")
        if "static" in uri or "/emoji.php/" in uri:
            continue
        if uri not in seen:
            listing.image_urls.append(uri)
            seen.add(uri)
            got = True

    if listing.image_urls and not listing.primary_image_url:
        listing.primary_image_url = listing.image_urls[0]
        listing.field_sources.setdefault("primary_image_url", "relay")

    return got


# --- L4: DOM fallback -------------------------------------------------------

RE_BRL = re.compile(r"R\$\s*[\d\.,]+")


def apply_dom_fallback(listing: Listing, soup: BeautifulSoup) -> bool:
    """Último recurso. Classes do FB são ofuscadas, então confiamos apenas em
    estrutura grosseira (h1 para título, regex de preço no texto visível)."""
    got = False
    if not listing.title:
        h1 = soup.find("h1")
        if h1:
            text = h1.get_text(strip=True)
            got |= _mark(listing, "title", text or None, "dom")

    if not listing.price_formatted:
        body = soup.find("body")
        if body:
            text = body.get_text(" ", strip=True)[:20000]
            m = RE_BRL.search(text)
            if m:
                got |= _mark(listing, "price_formatted", m.group(0), "dom")

    return got


# --- walk de JSONs embutidos (complemento da camada Relay) -------------------

def apply_json_walk(listing: Listing, soup: BeautifulSoup) -> bool:
    got = False
    for tag in soup.find_all("script", attrs={"type": "application/json"}):
        text = (tag.string or tag.get_text() or "").strip()
        if not text or text[0] not in "{[":
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        for node in _walk(payload):
            if not isinstance(node, dict):
                continue
            if "marketplace_listing_title" in node:
                v = node.get("marketplace_listing_title")
                if isinstance(v, str):
                    got |= _mark(listing, "title", v, "json_walk")
            lp = node.get("listing_price")
            if isinstance(lp, dict):
                got |= _mark(listing, "price_amount", lp.get("amount"), "json_walk")
                got |= _mark(listing, "price_currency", lp.get("currency"), "json_walk")
                got |= _mark(listing, "price_formatted", lp.get("formatted_amount"), "json_walk")
            rd = node.get("redacted_description")
            if isinstance(rd, dict) and isinstance(rd.get("text"), str):
                got |= _mark(listing, "description", rd["text"], "json_walk")
    return got


def _walk(obj: Any) -> Iterator[Any]:
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk(v)


# --- orquestrador puro (sem IO) ---------------------------------------------

def parse_html(html: str, item_id: str, url: str, final_url: str | None = None) -> Listing:
    listing = Listing(id=item_id, url=url, fetched_at=now_iso())

    if detect_not_found(html):
        listing.status = "not_found"
        listing.extraction_method = "not_found"
        log.info(kv(listing=item_id, status="not_found"))
        return listing

    if detect_login_wall(html, final_url or url):
        listing.status = "login_wall"
        listing.extraction_method = "login_wall"
        log.warning(kv(listing=item_id, status="login_wall"))
        return listing

    soup = BeautifulSoup(html, "html.parser")
    layers: list[str] = []

    if apply_jsonld(listing, soup):
        layers.append("jsonld")

    if apply_og(listing, parse_opengraph(soup)):
        layers.append("og")

    if apply_relay_regex(listing, html):
        layers.append("relay")

    if apply_json_walk(listing, soup):
        layers.append("json_walk")

    if apply_dom_fallback(listing, soup):
        layers.append("dom")

    listing.extraction_method = "+".join(layers) if layers else "none"
    if not listing.title and not listing.price_amount and not listing.price_formatted:
        listing.status = "empty"
        log.warning(kv(listing=item_id, status="empty", method=listing.extraction_method))
    else:
        log.info(kv(
            listing=item_id,
            status=listing.status,
            method=listing.extraction_method,
            price=listing.price_formatted or listing.price_amount,
            title_len=len(listing.title or ""),
        ))
    return listing


# --- fetchers ---------------------------------------------------------------

def _error_listing(item_id: str, url: str, method: str) -> Listing:
    return Listing(
        id=item_id, url=url, fetched_at=now_iso(),
        status="error", extraction_method=method,
    )


def extract(id_or_url: str, cache: bool = False) -> Listing:
    item_id, url = normalize_target(id_or_url)
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
    except requests.RequestException as e:
        log.error(kv(listing=item_id, event="fetch_error", error=type(e).__name__))
        return _error_listing(item_id, url, f"req_err:{type(e).__name__}")
    if r.status_code != 200:
        log.error(kv(listing=item_id, event="http_error", code=r.status_code))
        return _error_listing(item_id, url, f"http_{r.status_code}")
    if cache:
        save_html(item_id, r.text)
    return parse_html(r.text, item_id, url, final_url=r.url)


async def extract_async(
    id_or_url: str, client: httpx.AsyncClient, cache: bool = False
) -> Listing:
    item_id, url = normalize_target(id_or_url)
    try:
        r = await client.get(url, headers=HEADERS, timeout=TIMEOUT, follow_redirects=True)
    except httpx.RequestError as e:
        log.error(kv(listing=item_id, event="fetch_error", error=type(e).__name__))
        return _error_listing(item_id, url, f"req_err:{type(e).__name__}")
    if r.status_code != 200:
        log.error(kv(listing=item_id, event="http_error", code=r.status_code))
        return _error_listing(item_id, url, f"http_{r.status_code}")
    if cache:
        save_html(item_id, r.text)
    return parse_html(r.text, item_id, url, final_url=str(r.url))


def _print_debug_report(listing: Listing) -> None:
    print("=" * 60)
    print(f"  id:      {listing.id}")
    print(f"  status:  {listing.status}")
    print(f"  method:  {listing.extraction_method}")
    print("-" * 60)
    print("  field sources (which layer filled each field):")
    for attr in sorted(listing.field_sources):
        print(f"    {attr:22s} <- {listing.field_sources[attr]}")
    for attr in ("title", "price_amount", "price_currency", "price_formatted",
                 "location_text", "category", "creation_time"):
        val = getattr(listing, attr)
        if val is not None:
            shown = str(val)[:80]
            print(f"    {attr:22s} = {shown}")
    if listing.image_urls:
        print(f"    image_urls             = {len(listing.image_urls)} URLs")
    print("=" * 60)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("target", help="ID do item ou URL completa")
    ap.add_argument("--debug-html", action="store_true",
                    help="salva HTML bruto + JSON extraído em html_cache/ e imprime relatório de camadas")
    args = ap.parse_args()

    listing = extract(args.target, cache=args.debug_html)

    if args.debug_html:
        _print_debug_report(listing)
        save_listing_json(listing.id, asdict(listing))
        print(f"\n  saved: html_cache/{listing.id}.html")
        print(f"  saved: html_cache/{listing.id}.json")
    else:
        print(json.dumps(asdict(listing), ensure_ascii=False, indent=2))

    return 0 if listing.status == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
