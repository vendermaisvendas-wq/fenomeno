"""Testes de _is_repost — função pura."""

from datetime import timedelta

from repost_detector import _is_repost, _price_close


def _l(title, price, first_seen=None, removed=None, is_removed=False):
    return {
        "current_title": title,
        "current_price": price,
        "first_seen_at": first_seen,
        "removed_at": removed,
        "is_removed": is_removed,
    }


def test_is_repost_same_title_within_window():
    removed = _l("Hilux SRV 2020 diesel", "180000",
                 first_seen="2026-04-01T00:00:00+00:00",
                 removed="2026-04-05T00:00:00+00:00",
                 is_removed=True)
    new = _l("Hilux SRV 2020 diesel", "180000",
             first_seen="2026-04-07T00:00:00+00:00")
    assert _is_repost(removed, new, timedelta(days=14)) is True


def test_is_repost_different_title():
    removed = _l("Hilux SRV 2020 diesel", "180000",
                 first_seen="2026-04-01T00:00:00+00:00",
                 removed="2026-04-05T00:00:00+00:00",
                 is_removed=True)
    new = _l("Civic 2015 flex", "75000",
             first_seen="2026-04-06T00:00:00+00:00")
    assert _is_repost(removed, new, timedelta(days=14)) is False


def test_is_repost_outside_window():
    removed = _l("Hilux SRV 2020", "180000",
                 first_seen="2026-04-01T00:00:00+00:00",
                 removed="2026-04-05T00:00:00+00:00",
                 is_removed=True)
    # new 20 dias depois — fora da janela de 14 dias
    new = _l("Hilux SRV 2020", "180000",
             first_seen="2026-04-25T00:00:00+00:00")
    assert _is_repost(removed, new, timedelta(days=14)) is False


def test_is_repost_before_removal_is_rejected():
    removed = _l("Hilux SRV 2020", "180000",
                 first_seen="2026-04-01T00:00:00+00:00",
                 removed="2026-04-10T00:00:00+00:00",
                 is_removed=True)
    # new ANTES do removed → não é repost
    new = _l("Hilux SRV 2020", "180000",
             first_seen="2026-04-05T00:00:00+00:00")
    assert _is_repost(removed, new, timedelta(days=14)) is False


def test_is_repost_price_too_different():
    removed = _l("Hilux SRV 2020", "180000",
                 first_seen="2026-04-01T00:00:00+00:00",
                 removed="2026-04-05T00:00:00+00:00",
                 is_removed=True)
    new = _l("Hilux SRV 2020", "250000",  # 40% diff
             first_seen="2026-04-07T00:00:00+00:00")
    assert _is_repost(removed, new, timedelta(days=14)) is False


def test_price_close_within_tolerance():
    assert _price_close("100", "110") is True   # 10%
    assert _price_close("100", "115") is True   # 15% — na borda


def test_price_close_outside_tolerance():
    assert _price_close("100", "120") is False  # 20%
    assert _price_close("100", "200") is False


def test_price_close_none_permissive():
    # None de um dos lados → retorna True (não penaliza)
    assert _price_close(None, "100") is True
    assert _price_close("100", None) is True
