"""
Testes das heurísticas de opportunities.py (puras — não tocam o DB).
"""

from opportunities import (
    check_below_market, check_price_drop, check_short_description,
    check_urgency, _tokens,
)


def test_urgency_detects_keyword():
    flag = check_urgency("VENDO URGENTE: iPhone 13")
    assert flag is not None
    assert flag.rule == "urgency_keyword"
    assert "urgente" in flag.reason


def test_urgency_case_insensitive():
    assert check_urgency("IPHONE 13 PRECISO VENDER HOJE") is not None
    assert check_urgency("Desapego geladeira") is not None


def test_urgency_no_match():
    assert check_urgency("Bicicleta aro 29 em ótimo estado") is None


def test_short_description_flags_none():
    assert check_short_description(None) is not None
    assert check_short_description("").rule == "short_description"
    assert check_short_description("oi").rule == "short_description"


def test_short_description_accepts_long():
    assert check_short_description("A" * 100) is None


def test_price_drop_flags_15pct():
    flag = check_price_drop([1000.0, 950.0, 800.0])
    assert flag is not None
    assert flag.rule == "price_drop"


def test_price_drop_ignores_small():
    assert check_price_drop([1000.0, 990.0, 980.0]) is None


def test_price_drop_ignores_single_point():
    assert check_price_drop([1000.0]) is None


def test_below_market_flags_outlier():
    stats = {"hilux": (150000.0, 10000.0, 10)}  # mean, stdev, n
    tokens = _tokens("Toyota Hilux 2015 barata")
    assert "hilux" in tokens
    flag = check_below_market(100000.0, tokens, stats)
    assert flag is not None
    assert flag.rule == "below_market"


def test_below_market_ignores_in_range():
    stats = {"hilux": (150000.0, 10000.0, 10)}
    tokens = _tokens("Toyota Hilux SRV")
    assert check_below_market(145000.0, tokens, stats) is None


def test_below_market_needs_enough_samples():
    # n=2 < MIN_GROUP_SIZE (4) — não deve flagar mesmo com preço baixo
    stats = {"raro": (1000.0, 100.0, 2)}
    tokens = _tokens("Item raro outlier")
    assert check_below_market(500.0, tokens, stats) is None
