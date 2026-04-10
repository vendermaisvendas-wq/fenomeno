"""Testes da função pura compute_probability."""

from opportunity_predictor import compute_probability


def _row(**overrides):
    base = {
        "discount_percentage": 0,
        "liquidity_score": 0,
        "fraud_risk_score": 0,
        "price_outlier": 0,
        "current_title": "Item",
    }
    base.update(overrides)
    return base


def test_zero_signals_returns_low():
    # discount=0, liq=0, fraud=0, outlier=0
    # f4 (not_fraud) = 1 (fraud<50), f5 (not_outlier) = 1 → ~0.25 só de absence
    p = compute_probability(_row())
    assert 0.20 <= p <= 0.30


def test_max_discount():
    p = compute_probability(_row(discount_percentage=60))
    # f1 satura em 1.0; weighted 0.35 + f4 0.15 + f5 0.10 = 0.60
    assert 0.55 <= p <= 0.65


def test_max_liquidity():
    p = compute_probability(_row(liquidity_score=100))
    # f2=1*0.20 + f4*0.15 + f5*0.10 = 0.45
    assert 0.40 <= p <= 0.50


def test_high_fraud_zeroes_fraud_signal():
    p = compute_probability(_row(fraud_risk_score=80))
    # f4=0, mas f5=1 ainda; total = 0.10
    assert 0.05 <= p <= 0.15


def test_outlier_zeroes_outlier_signal():
    p = compute_probability(_row(price_outlier=1))
    # f5=0, mas f4=1; total = 0.15
    assert 0.10 <= p <= 0.20


def test_velocity_signal_via_index():
    velocity = {"iphone": 1.0}
    p = compute_probability(
        _row(current_title="iPhone 13"),
        token_velocity_index=velocity,
    )
    # f3=1*0.20 = 0.20; + f4*0.15 + f5*0.10 = 0.45
    assert 0.40 <= p <= 0.50


def test_full_max_combination():
    velocity = {"iphone": 1.0}
    p = compute_probability(
        _row(
            discount_percentage=60,
            liquidity_score=100,
            fraud_risk_score=10,
            price_outlier=0,
            current_title="iPhone 13",
        ),
        token_velocity_index=velocity,
    )
    # f1=1*0.35 + f2=1*0.20 + f3=1*0.20 + f4=1*0.15 + f5=1*0.10 = 1.0
    assert p >= 0.99


def test_clipped_in_range():
    p = compute_probability(_row(discount_percentage=999))
    assert 0.0 <= p <= 1.0
