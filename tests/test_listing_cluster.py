"""Testes das funções puras de listing_cluster."""

from listing_cluster import _connectable
from market_value import PricedItem


def _it(id_, toks, brand=None, year=None):
    return PricedItem(
        id=id_, price=1000, tokens=set(toks),
        brand=brand, year=year, title=" ".join(toks),
    )


def test_connectable_same_brand_similar_tokens():
    a = _it("a", ["hilux", "srv", "diesel"], brand="toyota", year=2020)
    b = _it("b", ["hilux", "srv", "diesel"], brand="toyota", year=2021)
    assert _connectable(a, b, eps=0.5) is True


def test_connectable_different_brand():
    a = _it("a", ["hilux", "srv"], brand="toyota", year=2020)
    b = _it("b", ["hilux", "srv"], brand="honda", year=2020)
    assert _connectable(a, b, eps=0.5) is False


def test_connectable_year_too_far():
    a = _it("a", ["hilux", "srv"], brand="toyota", year=2015)
    b = _it("b", ["hilux", "srv"], brand="toyota", year=2022)
    # diff = 7 > YEAR_TOLERANCE (2)
    assert _connectable(a, b, eps=0.5) is False


def test_connectable_low_jaccard():
    a = _it("a", ["hilux", "srv"], brand="toyota", year=2020)
    b = _it("b", ["corolla", "xei"], brand="toyota", year=2020)
    # jaccard = 0/4 → 0
    assert _connectable(a, b, eps=0.5) is False


def test_connectable_without_year_info():
    a = _it("a", ["hilux", "srv"], brand="toyota")
    b = _it("b", ["hilux", "sr"], brand="toyota")
    # jaccard = 1/3 = 0.33 → abaixo de 0.5
    assert _connectable(a, b, eps=0.5) is False
    # Com eps mais permissivo passa
    assert _connectable(a, b, eps=0.3) is True


def test_connectable_without_brand_relies_on_jaccard():
    a = _it("a", ["bicicleta", "aro", "29", "alumino"])
    b = _it("b", ["bicicleta", "aro", "29", "carbono"])
    # jaccard = 3/5 = 0.6
    assert _connectable(a, b, eps=0.5) is True
