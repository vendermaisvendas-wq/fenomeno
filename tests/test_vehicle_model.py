"""Testes de extract() e find_vehicle_comparables — puros."""

from market_value import PricedItem
from vehicle_model import extract, find_vehicle_comparables


def test_extract_full_hilux():
    f = extract("Toyota Hilux SRV 2013 Diesel 4x4 automatica")
    assert f.brand == "toyota"
    assert f.model == "hilux"
    assert f.year == 2013
    assert f.fuel == "diesel"
    assert f.transmission == "automatica"
    assert f.traction == "4x4"


def test_extract_civic_flex():
    f = extract("Honda Civic LXR 2015 flex")
    assert f.brand == "honda"
    assert f.model == "civic"
    assert f.year == 2015
    assert f.fuel == "flex"


def test_extract_engine_size():
    f = extract("Fiat Uno 1.0 2020 flex")
    assert f.engine == "1.0"
    assert f.brand == "fiat"
    assert f.model == "uno"
    assert f.year == 2020


def test_extract_moto_titan():
    f = extract("Honda CG Titan 150 2018")
    assert f.brand == "honda"
    assert f.year == 2018
    assert f.model in ("titan", "cg")


def test_extract_no_match():
    f = extract("Bicicleta aro 29")
    assert f.brand is None
    assert f.model is None


def test_extract_none_title():
    f = extract(None)
    assert f.brand is None


def _v(id_, title, price):
    from title_normalizer import extract_brand, extract_year, tokens
    return PricedItem(
        id=id_, price=price, tokens=tokens(title),
        brand=extract_brand(title), year=extract_year(title), title=title,
    )


def test_vehicle_comparables_prefers_same_model_year_fuel():
    target = _v("t", "Toyota Hilux SRV 2020 Diesel", 180000)
    pool = [
        target,
        _v("a", "Toyota Hilux SRV 2020 Diesel", 175000),
        _v("b", "Toyota Hilux SR 2020 Diesel", 170000),
        _v("c", "Toyota Hilux SRX 2020 Diesel", 185000),
        _v("d", "Toyota Hilux SRV 2020 flex", 165000),  # outro combustível
        _v("x", "Honda Civic 2020", 110000),
    ]
    comps = find_vehicle_comparables(target, pool)
    comp_ids = {c.id for c in comps}
    # a, b, c têm hilux + 2020 + diesel → devem estar
    assert {"a", "b", "c"} <= comp_ids
    # Honda Civic não deve
    assert "x" not in comp_ids


def test_vehicle_comparables_relaxes_year():
    # Só 1 mesmo ano com mesma fuel → precisa relaxar
    target = _v("t", "Honda Civic 2020 flex", 110000)
    pool = [
        target,
        _v("a", "Honda Civic 2018 flex", 100000),  # ano diferente
        _v("b", "Honda Civic 2019 flex", 105000),
        _v("c", "Honda Civic 2021 flex", 115000),
    ]
    comps = find_vehicle_comparables(target, pool)
    assert len(comps) >= 3


def test_vehicle_comparables_returns_empty_without_model():
    # Target sem modelo conhecido → cascata não aplica
    target = _v("t", "Honda generic", 50000)
    pool = [target, _v("a", "Honda Civic", 80000)]
    comps = find_vehicle_comparables(target, pool)
    assert comps == []
