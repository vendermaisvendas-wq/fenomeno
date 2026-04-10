"""Testes de title_normalizer — puros, sem DB."""

from title_normalizer import (
    extract_brand, extract_year, jaccard, normalize, signature, tokens,
)


def test_normalize_lowercase_and_strip_accents():
    assert normalize("Ônibus CORINGA") == "onibus coringa"
    assert normalize("São Paulo") == "sao paulo"
    assert normalize(None) == ""
    assert normalize("") == ""


def test_tokens_removes_stopwords_and_short():
    toks = tokens("Vendo iPhone 13 com carregador de fábrica")
    assert "iphone" in toks
    assert "13" in toks
    assert "carregador" in toks
    assert "fabrica" in toks
    # stopwords removidas
    assert "vendo" not in toks
    assert "com" not in toks
    assert "de" not in toks


def test_tokens_minimum_length():
    toks = tokens("A b cd efg")
    assert "cd" in toks
    assert "efg" in toks
    # muito curtos eliminados
    assert "a" not in toks
    assert "b" not in toks


def test_extract_year_valid_range():
    assert extract_year("Hilux 2013 diesel") == 2013
    assert extract_year("Honda CG 2024") == 2024
    assert extract_year("Moto 1985") == 1985
    assert extract_year("Sem ano aqui") is None


def test_extract_year_rejects_implausible():
    # Fora do range 1980-2049
    assert extract_year("Item 1950") is None
    assert extract_year("Item 2060") is None


def test_extract_year_picks_first_match():
    assert extract_year("Carro 2015 revisado até 2024") == 2015


def test_extract_brand_finds_known_brand():
    assert extract_brand("Toyota Hilux SRV 2020") == "toyota"
    assert extract_brand("iPhone 13 Pro") == "iphone"
    assert extract_brand("Honda CG Titan 160") == "honda"


def test_extract_brand_case_insensitive():
    assert extract_brand("YAMAHA FAZER 250") == "yamaha"


def test_extract_brand_returns_none_when_absent():
    assert extract_brand("Bicicleta aro 29") is None


def test_signature_separates_components():
    brand, toks, year = signature("Toyota Hilux SRV 2020 Diesel")
    assert brand == "toyota"
    assert year == 2020
    assert "hilux" in toks
    assert "srv" in toks
    assert "diesel" in toks
    # brand e year não devem aparecer no set de tokens
    assert "toyota" not in toks
    assert "2020" not in toks


def test_jaccard_identical_sets():
    assert jaccard({"a", "b", "c"}, {"a", "b", "c"}) == 1.0


def test_jaccard_disjoint_sets():
    assert jaccard({"a"}, {"b"}) == 0.0


def test_jaccard_partial_overlap():
    # 2 comuns de 4 únicos = 0.5
    assert jaccard({"a", "b", "c"}, {"a", "b", "d"}) == 0.5


def test_jaccard_empty_sets_both():
    assert jaccard(set(), set()) == 1.0
