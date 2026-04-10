"""Testes do classificador rule-based de categorias."""

from category_models import classify


def test_classify_vehicle_by_brand():
    assert classify("Toyota Hilux SRV 2020 diesel") == "vehicles"
    assert classify("Honda CG 160 Titan 2019") == "vehicles"


def test_classify_vehicle_by_generic_term():
    assert classify("Carro popular barato") == "vehicles"
    assert classify("Moto semi nova") == "vehicles"


def test_classify_electronics():
    assert classify("iPhone 13 128GB preto") == "electronics"
    assert classify("PlayStation 5 com 2 controles") == "electronics"
    assert classify("Notebook Dell i7") == "electronics"
    assert classify("Smart TV Samsung 55 polegadas") == "electronics"


def test_classify_real_estate():
    assert classify("Casa 3 quartos com quintal") == "real_estate"
    assert classify("Apartamento 2 suites") == "real_estate"
    assert classify("Terreno 400m2") == "real_estate"


def test_classify_furniture():
    assert classify("Sofá de canto 3 lugares") == "furniture"
    assert classify("Geladeira Brastemp 430 litros") == "furniture"
    assert classify("Guarda roupa 6 portas") == "furniture"


def test_classify_other_when_no_tokens_match():
    assert classify("Item raro colecionável") == "other"


def test_classify_empty_and_none():
    assert classify("") == "other"
    assert classify(None) == "other"


def test_classify_tie_breaker_priority():
    # Empate 1-1: "carro" só em vehicles, "lenovo" só em electronics
    # Prioridade CATEGORY_PRIORITY coloca vehicles primeiro
    result = classify("carro lenovo")
    assert result == "vehicles"


def test_classify_stronger_signal_wins():
    # electronics tem 3 hits, vehicles só 1 → electronics vence
    result = classify("iphone samsung xiaomi carro")
    assert result == "electronics"
