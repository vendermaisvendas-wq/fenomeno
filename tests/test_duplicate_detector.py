"""Testes da função pura is_similar() do duplicate_detector."""

from duplicate_detector import Item, UnionFind, _city_of, is_similar


def _mk(id_, toks, price, city=None):
    return Item(id=id_, tokens=set(toks), price=price, city=city)


def test_is_similar_identical_titles_same_price_same_city():
    a = _mk("a", ["iphone", "13", "128gb"], 3500, "sao paulo")
    b = _mk("b", ["iphone", "13", "128gb"], 3500, "sao paulo")
    assert is_similar(a, b) is True


def test_is_similar_different_tokens_below_threshold():
    a = _mk("a", ["iphone", "13"], 3500, "sao paulo")
    b = _mk("b", ["samsung", "s21"], 3500, "sao paulo")
    assert is_similar(a, b) is False


def test_is_similar_price_diff_above_tolerance():
    a = _mk("a", ["iphone", "13", "pro"], 3500, "sp")
    b = _mk("b", ["iphone", "13", "pro"], 4500, "sp")  # +28%
    assert is_similar(a, b) is False


def test_is_similar_price_diff_within_tolerance():
    a = _mk("a", ["iphone", "13", "pro"], 3500, "sp")
    b = _mk("b", ["iphone", "13", "pro"], 3700, "sp")  # +5.7%
    assert is_similar(a, b) is True


def test_is_similar_different_cities():
    a = _mk("a", ["iphone", "13", "pro"], 3500, "sao paulo")
    b = _mk("b", ["iphone", "13", "pro"], 3500, "rio de janeiro")
    assert is_similar(a, b) is False


def test_is_similar_no_city_info_allows_match():
    a = _mk("a", ["iphone", "13", "pro"], 3500, None)
    b = _mk("b", ["iphone", "13", "pro"], 3500, None)
    assert is_similar(a, b) is True


def test_is_similar_no_price_info_falls_back_to_title_and_city():
    a = _mk("a", ["iphone", "13", "pro"], None, "sp")
    b = _mk("b", ["iphone", "13", "pro"], None, "sp")
    assert is_similar(a, b) is True


def test_city_of_extracts_first_component():
    assert _city_of("São Paulo, SP") == "são paulo"
    assert _city_of("Rio de Janeiro, RJ") == "rio de janeiro"
    assert _city_of(None) is None
    assert _city_of("") is None


def test_union_find_basic():
    uf = UnionFind()
    for x in ["a", "b", "c", "d"]:
        uf.add(x)
    uf.union("a", "b")
    uf.union("b", "c")
    assert uf.find("a") == uf.find("c")
    assert uf.find("d") != uf.find("a")


def test_union_find_path_compression():
    uf = UnionFind()
    for x in range(10):
        uf.add(str(x))
    # encadeia 0-1-2-3-4
    for i in range(4):
        uf.union(str(i), str(i + 1))
    # todos no mesmo set
    root = uf.find("0")
    for i in range(5):
        assert uf.find(str(i)) == root
