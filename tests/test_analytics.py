"""
Testes do parser de preço em analytics._to_float.
"""

from analytics import _to_float, compute_stats


def test_to_float_brazilian_format():
    assert _to_float("R$ 1.234,56") == 1234.56
    assert _to_float("R$ 185.000") == 185000.0
    assert _to_float("1.234,56") == 1234.56


def test_to_float_international_format():
    assert _to_float("1234.56") == 1234.56
    assert _to_float("3500") == 3500.0


def test_to_float_handles_garbage():
    assert _to_float(None) is None
    assert _to_float("") is None
    assert _to_float("abc") is None


def test_compute_stats_empty_group():
    stats = compute_stats([], "hilux")
    assert stats.count == 0
    assert stats.mean is None


def test_compute_stats_basic():
    listings = [
        {"title": "Hilux SRV 2020", "price": "150000", "currency": "BRL"},
        {"title": "Hilux 2018", "price": "120000", "currency": "BRL"},
        {"title": "Hilux SR 2019", "price": "135000", "currency": "BRL"},
        {"title": "Corolla", "price": "80000", "currency": "BRL"},
    ]
    stats = compute_stats(listings, "hilux")
    assert stats.count == 3
    assert stats.mean == 135000.0
    assert stats.minimum == 120000.0
    assert stats.maximum == 150000.0
    assert stats.currency == "BRL"


def test_compute_stats_case_insensitive():
    listings = [{"title": "HILUX SRV", "price": "150000", "currency": "BRL"}]
    stats = compute_stats(listings, "hilux")
    assert stats.count == 1
