"""
Fallback com navegador real via Playwright.

Use somente quando extract_item.py retornar `status="empty"` ou faltar campos
importantes. Custo ~100x maior que requests — não use em loop apertado.

Instalação:
    pip install playwright
    playwright install chromium

Uso:
    python extract_item_playwright.py 2015275022700246
"""

from __future__ import annotations

import json
import sys
from dataclasses import asdict

from playwright.sync_api import sync_playwright

from extract_item import (
    Listing,
    apply_json_walk,
    apply_og,
    apply_regex,
    detect_login_wall,
    normalize_target,
    parse_opengraph,
)
from bs4 import BeautifulSoup
from datetime import datetime, timezone


def extract_with_browser(item_id_or_url: str) -> Listing:
    item_id, url = normalize_target(item_id_or_url)
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    listing = Listing(id=item_id, url=url, fetched_at=now_iso)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            locale="pt-BR",
            viewport={"width": 1280, "height": 900},
            # Sem stealth, sem mascaramento — rodamos como um Chromium normal.
        )
        page = ctx.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            # Pequena espera para o SSR terminar de entregar chunks ScheduledServerJS.
            page.wait_for_timeout(2_000)
            html = page.content()
            final_url = page.url
        finally:
            ctx.close()
            browser.close()

    if detect_login_wall(html, final_url):
        listing.status = "login_wall"
        listing.extraction_method = "playwright+login_wall"
        return listing

    soup = BeautifulSoup(html, "html.parser")
    og = parse_opengraph(soup)
    apply_og(listing, og)
    apply_regex(listing, html)
    apply_json_walk(listing, soup)

    listing.extraction_method = "playwright"
    if not listing.title and not listing.price_amount:
        listing.status = "empty"
    return listing


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    listing = extract_with_browser(sys.argv[1])
    print(json.dumps(asdict(listing), ensure_ascii=False, indent=2))
    return 0 if listing.status == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
