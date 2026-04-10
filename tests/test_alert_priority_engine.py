"""Testes da função pura compute_priority_score do alert_priority_engine."""

from alert_priority_engine import compute_priority_score


def _l(**overrides):
    base = {
        "opportunity_probability": 0.0,
        "discount_percentage": 0.0,
        "fresh_opportunity_score": 0,
        "liquidity_score": 0,
    }
    base.update(overrides)
    # Simula sqlite3.Row's keys() method
    class _Row(dict):
        def keys(self):
            return list(super().keys())
    return _Row(base)


def _w(plan=None):
    class _Row(dict):
        def keys(self):
            return list(super().keys())
    return _Row({"plan": plan, "watch_id": 1})


def test_zero_returns_zero():
    assert compute_priority_score(_l(), _w()) == 0.0


def test_high_probability_dominates():
    score = compute_priority_score(_l(opportunity_probability=1.0), _w())
    assert score == 50.0  # 1.0 * 50


def test_discount_capped():
    # discount * 0.5 cap em 30
    score = compute_priority_score(_l(discount_percentage=200), _w())
    assert score == 30.0


def test_fresh_capped():
    score = compute_priority_score(_l(fresh_opportunity_score=200), _w())
    assert score == 20.0


def test_liquidity_capped():
    score = compute_priority_score(_l(liquidity_score=200), _w())
    assert score == 10.0


def test_premium_plan_boost():
    listing = _l(opportunity_probability=1.0)
    base_score = compute_priority_score(listing, _w())
    premium_score = compute_priority_score(listing, _w(plan="premium"))
    assert premium_score == base_score * 1.5


def test_pro_plan_boost():
    listing = _l(opportunity_probability=1.0)
    base_score = compute_priority_score(listing, _w())
    pro_score = compute_priority_score(listing, _w(plan="pro"))
    assert pro_score == base_score * 1.2


def test_free_plan_no_boost():
    listing = _l(opportunity_probability=1.0)
    base_score = compute_priority_score(listing, _w())
    free_score = compute_priority_score(listing, _w(plan="free"))
    assert free_score == base_score


def test_full_combination():
    listing = _l(
        opportunity_probability=1.0,
        discount_percentage=60,
        fresh_opportunity_score=100,
        liquidity_score=100,
    )
    score = compute_priority_score(listing, _w(plan="premium"))
    # 50 + 30 + 20 + 10 = 110, * 1.5 = 165
    assert score == 165.0


def test_none_watcher_safe():
    score = compute_priority_score(
        _l(opportunity_probability=0.5),
        {},
    )
    assert score == 25.0  # 0.5 * 50, sem boost
