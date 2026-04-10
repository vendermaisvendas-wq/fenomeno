"""Testes do ComparablesIndex (índice invertido)."""

from market_value import ComparablesIndex, PricedItem


def _item(id_, price, toks, brand=None, year=None):
    return PricedItem(
        id=id_, price=price, tokens=set(toks),
        brand=brand, year=year, title=" ".join(toks),
    )


def test_index_builds_token_map():
    items = [
        _item("a", 100, ["hilux", "srv"]),
        _item("b", 200, ["hilux", "sr"]),
        _item("c", 50, ["civic"]),
    ]
    idx = ComparablesIndex(items)
    assert idx.by_token["hilux"] == {"a", "b"}
    assert idx.by_token["srv"] == {"a"}
    assert idx.by_token["civic"] == {"c"}


def test_index_builds_brand_buckets():
    items = [
        _item("a", 100, ["hilux"], brand="toyota", year=2020),
        _item("b", 110, ["hilux"], brand="toyota", year=2020),
        _item("c", 80, ["civic"], brand="honda", year=2020),
    ]
    idx = ComparablesIndex(items)
    assert {i.id for i in idx.by_brand["toyota"]} == {"a", "b"}
    assert {i.id for i in idx.by_brand["honda"]} == {"c"}
    assert {i.id for i in idx.by_brand_year[("toyota", 2020)]} == {"a", "b"}


def test_find_comparables_same_brand_year():
    items = [
        _item("a", 150, ["hilux"], brand="toyota", year=2020),
        _item("b", 145, ["hilux"], brand="toyota", year=2020),
        _item("c", 155, ["hilux"], brand="toyota", year=2020),
        _item("d", 160, ["hilux"], brand="toyota", year=2020),
        _item("e", 180, ["hilux"], brand="toyota", year=2022),
    ]
    idx = ComparablesIndex(items)
    comps = idx.find_comparables(items[0])
    assert {c.id for c in comps} == {"b", "c", "d"}


def test_find_comparables_caches_results():
    items = [
        _item("a", 150, ["hilux"], brand="toyota", year=2020),
        _item("b", 145, ["hilux"], brand="toyota", year=2020),
        _item("c", 155, ["hilux"], brand="toyota", year=2020),
        _item("d", 160, ["hilux"], brand="toyota", year=2020),
    ]
    idx = ComparablesIndex(items)
    first = idx.find_comparables(items[0])
    second = idx.find_comparables(items[0])
    assert first is second  # mesma lista cacheada


def test_find_comparables_jaccard_fallback_uses_invindex():
    # Sem marca → deve cair no fallback por tokens via invindex
    items = [
        _item("a", 1000, ["bicicleta", "aro", "29", "aluminio"]),
        _item("b", 950, ["bicicleta", "aro", "29", "carbono"]),
        _item("c", 1200, ["bicicleta", "aro", "26"]),
        _item("d", 500, ["geladeira", "brastemp"]),
    ]
    idx = ComparablesIndex(items)
    comps = idx.find_comparables(items[0])
    comp_ids = {c.id for c in comps}
    assert "b" in comp_ids  # jaccard >= 0.5
    assert "d" not in comp_ids  # sem token em comum


def test_find_comparables_excludes_self():
    items = [
        _item("a", 100, ["hilux"], brand="toyota", year=2020),
        _item("b", 110, ["hilux"], brand="toyota", year=2020),
        _item("c", 120, ["hilux"], brand="toyota", year=2020),
    ]
    idx = ComparablesIndex(items)
    comps = idx.find_comparables(items[0])
    assert all(c.id != "a" for c in comps)
