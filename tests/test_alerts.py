"""Testes das funções puras de alerts.py (sem chamar webhook real)."""

import alerts


def _row(**overrides):
    base = {
        "id": "abc",
        "url": "https://fb/marketplace/item/abc/",
        "current_title": "iPhone 13 128GB",
        "current_price": "3500",
        "current_currency": "BRL",
        "discount_percentage": None,
        "estimated_market_value": None,
        "opportunity_score": 0,
    }
    base.update(overrides)
    return base


def test_should_alert_high_score():
    assert alerts.should_alert(_row(opportunity_score=85)) is True


def test_should_alert_high_discount():
    assert alerts.should_alert(_row(discount_percentage=35.0)) is True


def test_should_alert_below_thresholds():
    assert alerts.should_alert(_row(opportunity_score=70, discount_percentage=20.0)) is False


def test_should_alert_none_values():
    assert alerts.should_alert(_row()) is False


def test_format_message_contains_essentials():
    row = _row(
        opportunity_score=85, discount_percentage=35.0,
        estimated_market_value=5000.0,
    )
    msg = alerts._format_message(row)
    assert "score=85" in msg
    assert "iPhone 13 128GB" in msg
    assert "3500" in msg
    assert "BRL" in msg
    assert "35%" in msg
    assert "5000" in msg
    assert "fb/marketplace/item/abc" in msg


def test_format_message_handles_missing_optionals():
    row = _row()
    msg = alerts._format_message(row)
    assert "iPhone 13 128GB" in msg
    # sem desconto/emv não devem poluir mensagem
    assert "desconto" not in msg
    assert "valor estimado" not in msg


def test_send_without_env_returns_none(monkeypatch):
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    assert alerts.send_telegram("test") is None
    assert alerts.send_discord("test") is None
