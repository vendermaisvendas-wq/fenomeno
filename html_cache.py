"""
Cache opcional de HTML bruto para debug e reprocessamento offline.

Layout:
    html_cache/
        {listing_id}.html     ← HTML completo como recebido do FB
        {listing_id}.json     ← Listing extraído (opcional, --debug-html)

Uso:
    from html_cache import save_html, load_html
    save_html("2015275022700246", html_text)
    html = load_html("2015275022700246")   # None se não existir
"""

from __future__ import annotations

import json
from pathlib import Path

CACHE_DIR = Path("html_cache")


def ensure_dir() -> None:
    CACHE_DIR.mkdir(exist_ok=True)


def html_path(listing_id: str) -> Path:
    return CACHE_DIR / f"{listing_id}.html"


def json_path(listing_id: str) -> Path:
    return CACHE_DIR / f"{listing_id}.json"


def save_html(listing_id: str, html: str) -> Path:
    ensure_dir()
    p = html_path(listing_id)
    p.write_text(html, encoding="utf-8")
    return p


def save_listing_json(listing_id: str, payload: dict) -> Path:
    ensure_dir()
    p = json_path(listing_id)
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return p


def load_html(listing_id: str) -> str | None:
    p = html_path(listing_id)
    if not p.exists():
        return None
    return p.read_text(encoding="utf-8")


def cached_ids() -> list[str]:
    ensure_dir()
    return sorted(p.stem for p in CACHE_DIR.glob("*.html"))


def clear(listing_id: str | None = None) -> int:
    """Remove tudo (se None) ou apenas um listing. Retorna count removido."""
    ensure_dir()
    if listing_id is None:
        count = 0
        for p in CACHE_DIR.iterdir():
            if p.is_file():
                p.unlink()
                count += 1
        return count
    removed = 0
    for p in (html_path(listing_id), json_path(listing_id)):
        if p.exists():
            p.unlink()
            removed += 1
    return removed
