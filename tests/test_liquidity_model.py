"""Testes da função pura compute_liquidity."""

from liquidity_model import Signal, compute_liquidity


def _row(**overrides):
    base = {
        "current_title": "iPhone 13 128GB preto",
        "discount_percentage": None,
        "opportunity_score": None,
        "cluster_id": None,
    }
    base.update(overrides)
    return base


def test_all_zero_signals_returns_zero():
    score, signals = compute_liquidity(
        _row(), None, cluster_sizes={}, velocity={},
    )
    assert score == 0


def test_max_discount_contributes_30():
    score, _ = compute_liquidity(
        _row(discount_percentage=50.0),  # > saturação 40
        None, cluster_sizes={}, velocity={},
    )
    assert score == 30


def test_max_opp_score_contributes_25():
    score, _ = compute_liquidity(
        _row(opportunity_score=100),
        None, cluster_sizes={}, velocity={},
    )
    assert score == 25


def test_long_description_contributes_10():
    score, _ = compute_liquidity(
        _row(), {"description": "A" * 300}, cluster_sizes={}, velocity={},
    )
    assert score == 10


def test_big_cluster_contributes_10():
    score, _ = compute_liquidity(
        _row(cluster_id=7), None,
        cluster_sizes={7: 20}, velocity={},
    )
    assert score == 10


def test_fast_token_velocity_contributes_25():
    score, _ = compute_liquidity(
        _row(), None,
        cluster_sizes={},
        velocity={"iphone": 1.0},  # max liquidez
    )
    assert score == 25


def test_all_signals_max_clips_at_100():
    score, _ = compute_liquidity(
        _row(
            discount_percentage=60,
            opportunity_score=100,
            cluster_id=1,
        ),
        {"description": "A" * 500},
        cluster_sizes={1: 50},
        velocity={"iphone": 1.0},
    )
    assert score == 100


def test_signals_list_populated():
    _, signals = compute_liquidity(
        _row(discount_percentage=30),
        None, cluster_sizes={}, velocity={},
    )
    assert any(isinstance(s, Signal) and s.name == "discount" for s in signals)
