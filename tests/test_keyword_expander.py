"""Testes do keyword_expander — função pura."""

from keyword_expander import expand


def test_expand_includes_original():
    out = expand("iphone")
    assert "iphone" in out
    assert out[0] == "iphone"


def test_expand_iphone_versions():
    out = expand("iphone", max_variations=10)
    assert "iphone 11" in out
    assert "iphone 13" in out


def test_expand_brand_added():
    out = expand("iphone", max_variations=15)
    assert "iphone apple" in out


def test_expand_modifiers():
    out = expand("iphone", max_variations=20)
    assert "iphone usado" in out
    assert "iphone seminovo" in out


def test_expand_unknown_keyword_only_modifiers():
    out = expand("escrivaninha", max_variations=10)
    assert out[0] == "escrivaninha"
    assert "escrivaninha usado" in out
    # Sem version expansions, sem brand expansions
    assert all("apple" not in v for v in out)


def test_expand_normalization():
    out = expand("  IPHONE  ")
    assert "iphone" == out[0]


def test_expand_empty_returns_empty():
    assert expand("") == []
    assert expand("   ") == []


def test_expand_respects_max_variations():
    out = expand("iphone", max_variations=3)
    assert len(out) <= 3
    assert out[0] == "iphone"


def test_expand_no_duplicates():
    out = expand("iphone", max_variations=20)
    assert len(out) == len(set(out))


def test_expand_synonyms():
    out = expand("notebook", max_variations=15)
    assert "laptop" in out


def test_expand_playstation_versions():
    out = expand("playstation", max_variations=10)
    assert "playstation 4" in out
    assert "playstation 5" in out


def test_expand_motorcycle_models():
    out = expand("cg", max_variations=15)
    assert "cg titan" in out or "cg 160" in out
