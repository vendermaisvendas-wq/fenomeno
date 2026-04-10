"""
Fixtures HTML sintéticas para testar cada camada do parser isoladamente.
Usamos strings construídas à mão para ter controle total sobre o que cada
camada "vê". Isso garante que mudanças no FB não invalidem os testes — eles
testam o parser, não a realidade do FB.
"""

import sys
from pathlib import Path

import pytest

# Garante que o diretório raiz do projeto está no sys.path quando rodamos
# `pytest` a partir de qualquer lugar
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def html_jsonld():
    return """
    <html><head>
    <script type="application/ld+json">
    {
        "@type": "Product",
        "name": "Moto Honda CG 160 Titan 2021",
        "description": "Moto em perfeito estado, revisada.",
        "image": ["https://example.com/moto1.jpg", "https://example.com/moto2.jpg"],
        "offers": {
            "@type": "Offer",
            "price": "13500",
            "priceCurrency": "BRL"
        }
    }
    </script>
    </head><body><h1>título DOM genérico</h1></body></html>
    """


@pytest.fixture
def html_og():
    return """
    <html><head>
    <meta property="og:title" content="iPhone 13 128GB preto | Facebook Marketplace" />
    <meta property="og:description" content="iPhone 13 128GB, bateria 92%, sem marcas." />
    <meta property="og:image" content="https://scontent.xx.fbcdn.net/v/t45.5328/iphone.jpg" />
    <meta property="og:url" content="https://www.facebook.com/marketplace/item/12345/" />
    <meta property="og:type" content="product.item" />
    <meta property="product:price:amount" content="3500" />
    <meta property="product:price:currency" content="BRL" />
    </head><body></body></html>
    """


@pytest.fixture
def html_relay():
    """Simula o payload que o ScheduledServerJS injetaria. Mantemos as chaves
    literais que os regex da camada Relay procuram."""
    blob = (
        '{"marketplace_listing_title":"Toyota Hilux SRV 2020 Diesel",'
        '"listing_price":{"amount":"185000","currency":"BRL",'
        '"formatted_amount":"R$ 185.000"},'
        '"redacted_description":{"text":"Hilux SRV 2020, completa, única dona, 78mil km."},'
        '"location_text":{"text":"São Paulo, SP"},'
        '"marketplace_listing_category_name":"Carros",'
        '"creation_time":1712764800,'
        '"marketplace_listing_seller":{"name":"João Silva","id":"42"},'
        '"primary_listing_photo":{"image":{"uri":"https://scontent.xx.fbcdn.net/hilux1.jpg"}}'
        '}'
    )
    return f'<html><body><script type="application/json">{blob}</script></body></html>'


@pytest.fixture
def html_dom_only():
    """Página sem OG, sem JSON-LD, sem Relay — só h1 e texto visível com preço."""
    return """
    <html><head><title>Algum anúncio</title></head>
    <body>
      <h1>Bicicleta aro 29 seminova</h1>
      <p>Em ótimo estado, pouco uso. Valor: R$ 1.250,00</p>
    </body></html>
    """


@pytest.fixture
def html_login_wall():
    return """
    <html><body>
      <form id="login_form" action="/login">
        <input name="email"><input name="pass">
      </form>
    </body></html>
    """


@pytest.fixture
def html_not_found():
    return "<html><body><h2>This content isn't available right now</h2></body></html>"


@pytest.fixture
def html_empty():
    return "<html><body><div>nada relevante aqui</div></body></html>"
