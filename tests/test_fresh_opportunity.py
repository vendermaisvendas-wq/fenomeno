"""Testes de compute_fresh_score."""

from datetime import datetime, timedelta, timezone

from fresh_opportunity_detector import compute_fresh_score


def _l(**overrides):
    base = {
        "current_title": "Item generic",
        "discount_percentage": None,
        "liquidity_score": None,
        "opportunity_score": None,
    }
    base.update(overrides)
    return base


def test_zero_if_outside_window():
    now = datetime.now(timezone.utc)
    old = (now - timedelta(hours=2)).isoformat()
    score = compute_fresh_score(
        _l(first_seen_at=old, discount_percentage=50, opportunity_score=90),
        now, window_minutes=30,
    )
    assert score == 0


def test_inside_window_with_big_discount():
    now = datetime.now(timezone.utc)
    recent = (now - timedelta(minutes=10)).isoformat()
    score = compute_fresh_score(
        _l(first_seen_at=recent, discount_percentage=40),
        now, window_minutes=30,
    )
    # discount_big = 40
    assert score == 40


def test_inside_window_with_mid_discount():
    now = datetime.now(timezone.utc)
    recent = (now - timedelta(minutes=5)).isoformat()
    score = compute_fresh_score(
        _l(first_seen_at=recent, discount_percentage=20),
        now, window_minutes=30,
    )
    assert score == 20


def test_high_liquidity_contributes_30():
    now = datetime.now(timezone.utc)
    recent = (now - timedelta(minutes=5)).isoformat()
    score = compute_fresh_score(
        _l(first_seen_at=recent, liquidity_score=70),
        now, window_minutes=30,
    )
    assert score == 30


def test_high_opportunity_contributes_20():
    now = datetime.now(timezone.utc)
    recent = (now - timedelta(minutes=5)).isoformat()
    score = compute_fresh_score(
        _l(first_seen_at=recent, opportunity_score=80),
        now, window_minutes=30,
    )
    assert score == 20


def test_popular_keyword_contributes_10():
    now = datetime.now(timezone.utc)
    recent = (now - timedelta(minutes=5)).isoformat()
    score = compute_fresh_score(
        _l(first_seen_at=recent, current_title="iPhone 13 128GB"),
        now, window_minutes=30,
    )
    assert score == 10  # só popular_keyword


def test_all_signals_max_clips_at_100():
    now = datetime.now(timezone.utc)
    recent = (now - timedelta(minutes=5)).isoformat()
    score = compute_fresh_score(
        _l(first_seen_at=recent,
           current_title="iPhone 13 128GB",
           discount_percentage=50,
           liquidity_score=80,
           opportunity_score=90),
        now, window_minutes=30,
    )
    # 40 + 30 + 20 + 10 = 100
    assert score == 100


def test_naive_timestamp_handled():
    now = datetime.now(timezone.utc)
    # ISO sem tz → função deve tratar como UTC
    naive = (now - timedelta(minutes=10)).replace(tzinfo=None).isoformat()
    score = compute_fresh_score(
        _l(first_seen_at=naive, discount_percentage=40),
        now, window_minutes=30,
    )
    assert score == 40
