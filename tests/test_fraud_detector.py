"""Testes das regras puras de fraud_detector.compute_fraud_score."""

from fraud_detector import compute_fraud_score


def _row(**overrides):
    base = {
        "current_title": "iPhone 13 128GB preto seminovo bateria 90%",
        "current_price": "3500",
        "current_currency": "BRL",
        "current_location": "São Paulo, SP",
        "estimated_market_value": 4000.0,
        "discount_percentage": 12.0,
    }
    base.update(overrides)
    return base


def _payload(**overrides):
    base = {
        "description": "iPhone 13 em ótimo estado, sem marcas de uso, acompanha caixa original.",
        "image_urls": ["url1", "url2", "url3"],
    }
    base.update(overrides)
    return base


def test_clean_listing_has_low_score():
    result = compute_fraud_score(_row(), _payload())
    assert result.score < 20
    # Pode ter pequenos hits mas não sinal forte


def test_absurdly_cheap_flagged():
    # preço 500 vs estimado 4000 → 12.5% do mercado
    result = compute_fraud_score(
        _row(current_price="500"),
        _payload(),
    )
    assert any("absurdly_cheap" in r for r in result.reasons)
    assert result.score >= 25


def test_few_images_flagged():
    result = compute_fraud_score(_row(), _payload(image_urls=["solo"]))
    assert any("few_images" in r for r in result.reasons)


def test_no_payload_gets_partial_few_images():
    result = compute_fraud_score(_row(), None)
    assert any("few_images" in r for r in result.reasons)


def test_short_description_flagged():
    result = compute_fraud_score(_row(), _payload(description="oi"))
    assert "short_description" in result.reasons


def test_short_title_flagged():
    result = compute_fraud_score(_row(current_title="vendo"), _payload())
    assert any("short_title" in r for r in result.reasons)


def test_generic_title_flagged():
    # "novo" e "a" são stopwords → restam 0 tokens úteis
    result = compute_fraud_score(
        _row(current_title="novo a"),
        _payload(),
    )
    assert "generic_title" in result.reasons


def test_missing_location_flagged():
    result = compute_fraud_score(
        _row(current_location=None),
        _payload(),
    )
    assert "no_location" in result.reasons


def test_huge_discount_plus_urgency_combo():
    result = compute_fraud_score(
        _row(discount_percentage=55.0,
             current_title="VENDO URGENTE iPhone hoje"),
        _payload(),
    )
    assert "huge_discount_plus_urgency" in result.reasons


def test_score_clips_at_100():
    # Força todos os sinais
    result = compute_fraud_score(
        {
            "current_title": "abc",  # short + generic
            "current_price": "100",
            "current_currency": "BRL",
            "current_location": None,
            "estimated_market_value": 10000.0,
            "discount_percentage": 90.0,
        },
        None,
    )
    assert result.score <= 100
