"""Testes de parse_location — função pura."""

from geo_coverage import _compute_coverage_score, parse_location


def test_parse_city_state_comma_format():
    city, state = parse_location("São Paulo, SP")
    assert city == "São Paulo"
    assert state == "SP"


def test_parse_city_state_with_country():
    city, state = parse_location("Rio de Janeiro, RJ, Brasil")
    assert city == "Rio De Janeiro"
    assert state == "RJ"


def test_parse_dash_format():
    city, state = parse_location("Belo Horizonte - MG")
    assert city == "Belo Horizonte"
    assert state == "MG"


def test_parse_full_state_name():
    city, state = parse_location("Campinas, São Paulo")
    assert city == "Campinas"
    assert state == "SP"


def test_parse_city_only():
    city, state = parse_location("Brasília")
    assert city == "Brasília"
    assert state is None


def test_parse_state_only():
    city, state = parse_location("RJ")
    assert city is None
    assert state == "RJ"


def test_parse_none_input():
    city, state = parse_location(None)
    assert city is None
    assert state is None


def test_parse_empty_input():
    assert parse_location("") == (None, None)


def test_coverage_score_volume_only():
    # 10 ativos, 5 tokens, recente → score moderado
    score = _compute_coverage_score(active=10, distinct_tokens=5, days_since_last=1)
    assert 0 < score < 100


def test_coverage_score_zero_volume():
    score = _compute_coverage_score(active=0, distinct_tokens=0, days_since_last=0)
    assert score <= 20  # só freshness


def test_coverage_score_saturation():
    # valores altíssimos devem saturar em 100
    score = _compute_coverage_score(active=10000, distinct_tokens=5000, days_since_last=0)
    assert score == 100


def test_coverage_score_stale_freshness():
    # 60 dias → fresh component = 0
    score = _compute_coverage_score(active=100, distinct_tokens=50, days_since_last=60)
    assert score < 100  # não pode ser 100 sem freshness
