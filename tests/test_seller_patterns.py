"""Testes da heurística _compute_reliability."""

from seller_patterns import _compute_reliability


def test_clean_seller_full_score():
    score = _compute_reliability(
        total=5, dup_count=0, fraud_avg=10.0, removed=1,
    )
    assert score == 100


def test_high_duplicate_ratio_penalizes():
    # dup_count / total = 0.40 > HIGH_DUP_RATIO (0.30)
    score = _compute_reliability(
        total=10, dup_count=4, fraud_avg=None, removed=0,
    )
    assert score == 80  # -20


def test_flooder_penalty():
    # total > 50 → -10
    score = _compute_reliability(
        total=60, dup_count=0, fraud_avg=None, removed=0,
    )
    assert score == 90


def test_high_fraud_avg_penalty():
    # fraud_avg > 50 → -25
    score = _compute_reliability(
        total=10, dup_count=0, fraud_avg=60.0, removed=0,
    )
    assert score == 75


def test_churn_penalty():
    # removed/total = 0.8 > 0.7 → -15
    score = _compute_reliability(
        total=10, dup_count=0, fraud_avg=None, removed=8,
    )
    assert score == 85


def test_all_penalties_stack():
    score = _compute_reliability(
        total=100, dup_count=50, fraud_avg=70.0, removed=80,
    )
    # -20 (dup) -10 (flood) -25 (fraud) -15 (churn) = 30
    assert score == 30


def test_score_floor_at_zero():
    # Hipotético: penalidades ultrapassariam -100
    score = _compute_reliability(
        total=1000, dup_count=999, fraud_avg=99.0, removed=999,
    )
    assert score >= 0


def test_low_volume_no_churn_penalty():
    # total <= 5 não dispara churn check mesmo com muito removed
    score = _compute_reliability(
        total=4, dup_count=0, fraud_avg=None, removed=4,
    )
    assert score == 100
