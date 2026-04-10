"""Testes do related_listing_finder — função pura."""

from related_listing_finder import derive_queries


def test_derive_iphone_with_storage():
    qs = derive_queries("iPhone 12 128GB preto bateria 90%")
    # deve incluir "iphone" (brand) — modelo "12" não é um "model" KNOWN
    assert "iphone" in qs


def test_derive_hilux_with_brand_model():
    qs = derive_queries("Toyota Hilux SRV 2020 Diesel 4x4 Automatica")
    assert "toyota hilux" in qs  # brand + model
    assert "hilux" in qs           # só model
    assert "toyota" in qs          # só brand


def test_derive_ordering_brand_model_first():
    qs = derive_queries("Toyota Hilux SRV 2020 Diesel")
    assert qs[0] == "toyota hilux"


def test_derive_max_queries_respected():
    qs = derive_queries("Toyota Hilux SRV 2020 Diesel 4x4", max_queries=3)
    assert len(qs) <= 3


def test_derive_dedup():
    qs = derive_queries("Honda Honda Honda CG 160 Titan 2020 cg")
    assert len(qs) == len(set(qs))


def test_derive_no_year_in_queries():
    qs = derive_queries("Honda CG Titan 2020")
    # 2020 nunca deve aparecer sozinho como query
    assert "2020" not in qs
    assert all("2020" != q for q in qs)


def test_derive_empty_returns_empty():
    assert derive_queries("") == []
    assert derive_queries(None) == []
    assert derive_queries("   ") == []


def test_derive_unknown_item_falls_back_to_tokens():
    qs = derive_queries("Bicicleta aro 29 alumínio carbono")
    # sem brand/model conhecido, usa par de tokens significativos
    assert len(qs) >= 1
    # tokens curtos não devem estar (aro tem 3 chars, OK)
    assert all(len(q) >= 3 for q in qs)


def test_derive_civic_model():
    qs = derive_queries("Honda Civic 2018 EXL flex")
    assert "honda civic" in qs
    assert "civic" in qs
