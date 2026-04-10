"""Testes de opportunities.compute_score (pura, sem DB)."""

from datetime import datetime, timedelta, timezone

from opportunities import compute_score


def _row(**overrides):
    """Row mock no formato sqlite3.Row (basta ser subscriptable)."""
    base = {
        "current_title": "Item generic",
        "discount_percentage": None,
        "first_seen_at": (datetime.now(timezone.utc) - timedelta(days=10)).isoformat(),
    }
    base.update(overrides)
    return base


def test_score_baseline_zero():
    # sem sinais → score 0
    row = _row()
    score, reasons = compute_score(row, {"description": "A" * 100})
    assert score == 0
    assert reasons == []


def test_score_big_discount_counts():
    row = _row(discount_percentage=35.0)
    score, reasons = compute_score(row, {"description": "A" * 100})
    # 40 (discount>30) + 15 (below_p25_proxy, discount>25) = 55
    assert score == 55
    assert any("discount>30" in r for r in reasons)
    assert "below_p25_proxy" in reasons


def test_score_mid_discount_counts():
    row = _row(discount_percentage=20.0)
    score, _ = compute_score(row, {"description": "A" * 100})
    assert score == 20  # mid only


def test_score_urgency_keyword():
    row = _row(current_title="VENDO URGENTE iPhone")
    score, reasons = compute_score(row, {"description": "A" * 100})
    assert score == 15
    assert "urgency_keyword" in reasons


def test_score_short_description():
    row = _row()
    score, reasons = compute_score(row, {"description": "oi"})
    assert score == 10
    assert "short_description" in reasons


def test_score_missing_description_also_triggers_short():
    row = _row()
    score, _ = compute_score(row, None)
    assert score == 10


def test_score_recent_listing():
    recent = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
    row = _row(first_seen_at=recent)
    score, reasons = compute_score(row, {"description": "A" * 100})
    assert score == 20
    assert "recent<2h" in reasons


def test_score_stacks_and_clips_to_100():
    recent = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    row = _row(
        current_title="VENDO URGENTE HOJE iPhone desapego",
        discount_percentage=40.0,
        first_seen_at=recent,
    )
    # 40 + 15 + 15 + 10 + 20 = 100 exato
    score, _ = compute_score(row, None)
    assert score == 100


def test_score_never_exceeds_100():
    # caso hipotético: score somaria 110, mas deve ser clippado
    recent = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    row = _row(
        current_title="URGENTE",
        discount_percentage=50.0,
        first_seen_at=recent,
    )
    score, _ = compute_score(row, None)
    assert score <= 100
