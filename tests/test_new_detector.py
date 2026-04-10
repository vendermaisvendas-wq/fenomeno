"""Testes das funções puras de new_listing_detector."""

from datetime import datetime, timedelta, timezone

from new_listing_detector import has_popular_keyword, is_recent


def test_is_recent_within_window():
    now = datetime.now(timezone.utc)
    past = (now - timedelta(minutes=30)).isoformat()
    assert is_recent(past, now, hours=2) is True


def test_is_recent_outside_window():
    now = datetime.now(timezone.utc)
    past = (now - timedelta(hours=5)).isoformat()
    assert is_recent(past, now, hours=2) is False


def test_is_recent_handles_naive_timestamps():
    # ISO sem tz → assume UTC
    now = datetime.now(timezone.utc)
    past = now.replace(tzinfo=None).isoformat()
    assert is_recent(past, now, hours=1) is True


def test_has_popular_keyword_matches_iphone():
    assert has_popular_keyword("Vendo iPhone 13 novo") is True


def test_has_popular_keyword_matches_vehicle():
    assert has_popular_keyword("Toyota Hilux SRV 2020 diesel") is True


def test_has_popular_keyword_misses_unknown():
    assert has_popular_keyword("Cadeira de escritório giratória") is False


def test_has_popular_keyword_empty():
    assert has_popular_keyword(None) is False
    assert has_popular_keyword("") is False
