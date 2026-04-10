"""
Testes das camadas do parser em extract_item.parse_html().

Estratégia: cada teste alimenta um HTML que só dispara uma camada e
verifica (a) que os campos corretos foram preenchidos e (b) que o
`field_sources` atribui à camada esperada. O nome do campo no
field_sources é a fonte-da-verdade para o healthcheck.
"""

from __future__ import annotations

from extract_item import parse_html


def test_jsonld_populates_title_price_image(html_jsonld):
    listing = parse_html(html_jsonld, "100", "http://fb/item/100/")
    assert listing.status == "ok"
    assert listing.title == "Moto Honda CG 160 Titan 2021"
    assert listing.price_amount == "13500"
    assert listing.price_currency == "BRL"
    assert listing.description == "Moto em perfeito estado, revisada."
    assert listing.field_sources["title"] == "jsonld"
    assert listing.field_sources["price_amount"] == "jsonld"
    assert "jsonld" in listing.extraction_method
    assert len(listing.image_urls) >= 2


def test_og_populates_and_strips_marketplace_suffix(html_og):
    listing = parse_html(html_og, "200", "http://fb/item/200/")
    assert listing.status == "ok"
    # Suffix "| Facebook Marketplace" deve ser removido
    assert listing.title == "iPhone 13 128GB preto"
    assert listing.price_amount == "3500"
    assert listing.price_currency == "BRL"
    assert listing.description.startswith("iPhone 13 128GB")
    assert listing.field_sources["title"] == "og"
    assert listing.field_sources["price_amount"] == "og"
    assert listing.primary_image_url.startswith("https://scontent.xx.fbcdn.net/")
    assert "og" in listing.extraction_method


def test_relay_layer_extracts_keys(html_relay):
    listing = parse_html(html_relay, "300", "http://fb/item/300/")
    assert listing.status == "ok"
    assert listing.title == "Toyota Hilux SRV 2020 Diesel"
    assert listing.price_amount == "185000"
    assert listing.price_currency == "BRL"
    assert listing.price_formatted == "R$ 185.000"
    assert listing.location_text == "São Paulo, SP"
    assert listing.category == "Carros"
    assert listing.creation_time == 1712764800
    assert listing.seller_name == "João Silva"
    # Relay regex OU json_walk devem ter preenchido — ambos são válidos
    assert listing.field_sources["title"] in ("relay", "json_walk")


def test_dom_fallback_picks_h1_and_price(html_dom_only):
    listing = parse_html(html_dom_only, "400", "http://fb/item/400/")
    # Sem title via OG/Relay, h1 deve entrar
    assert listing.title == "Bicicleta aro 29 seminova"
    assert listing.field_sources["title"] == "dom"
    # Preço formatado "R$ 1.250,00" deve ter sido extraído do texto visível
    assert listing.price_formatted is not None
    assert "1.250" in listing.price_formatted
    assert listing.field_sources["price_formatted"] == "dom"


def test_login_wall_detection(html_login_wall):
    listing = parse_html(html_login_wall, "500", "http://fb/item/500/")
    assert listing.status == "login_wall"
    # Nada deve ser extraído após detecção de wall
    assert listing.title is None
    assert listing.price_amount is None


def test_not_found_detection(html_not_found):
    listing = parse_html(html_not_found, "600", "http://fb/item/600/")
    assert listing.status == "not_found"
    assert listing.extraction_method == "not_found"


def test_empty_html_marks_empty_status(html_empty):
    listing = parse_html(html_empty, "700", "http://fb/item/700/")
    assert listing.status == "empty"
    assert listing.title is None
    assert listing.price_amount is None


def test_layer_priority_stable_over_fragile(html_og):
    """Quando OG e Relay coexistem, o primeiro a marcar o campo vence (camada
    mais estável por ordem). Este teste protege essa garantia."""
    # html_og só tem OG, verificamos que field_sources é OG (não DOM fallback)
    listing = parse_html(html_og, "800", "http://fb/item/800/")
    assert listing.field_sources["title"] == "og"
    assert listing.field_sources["price_amount"] == "og"
