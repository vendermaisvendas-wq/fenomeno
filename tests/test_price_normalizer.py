"""Testes do parser canônico de preços."""

import pytest

from price_normalizer import parse


# --- formato BR completo ---------------------------------------------------

def test_br_with_currency_and_thousands_and_decimal():
    assert parse("R$ 1.234,56") == 1234.56


def test_br_thousands_without_decimal():
    assert parse("R$ 185.000") == 185000.0
    assert parse("185.000") == 185000.0


def test_br_with_symbol_and_no_space():
    assert parse("R$185000") == 185000.0
    assert parse("R$185.000") == 185000.0


# --- formato internacional -------------------------------------------------

def test_intl_decimal():
    assert parse("1234.56") == 1234.56


def test_plain_integer():
    assert parse("185000") == 185000.0
    assert parse("3500") == 3500.0


def test_numeric_input_passthrough():
    assert parse(1234.56) == 1234.56
    assert parse(3500) == 3500.0


# --- sufixo "k" ------------------------------------------------------------

def test_suffix_k_integer():
    assert parse("185k") == 185_000.0
    assert parse("3k") == 3_000.0


def test_suffix_k_with_decimal():
    assert parse("1.5k") == 1_500.0
    assert parse("2,5k") == 2_500.0


def test_suffix_k_case_insensitive():
    assert parse("185K") == 185_000.0


def test_suffix_k_with_currency():
    assert parse("R$ 185k") == 185_000.0


# --- sufixo "mil" ----------------------------------------------------------

def test_suffix_mil():
    assert parse("185 mil") == 185_000.0
    assert parse("3 mil") == 3_000.0


def test_suffix_mil_with_decimal():
    assert parse("1,5 mil") == 1_500.0


def test_suffix_mil_with_currency():
    assert parse("R$ 185 mil") == 185_000.0


# --- sufixo milhões --------------------------------------------------------

def test_suffix_milhoes():
    assert parse("2 milhões") == 2_000_000.0
    assert parse("2 milhoes") == 2_000_000.0
    assert parse("2 milhão") == 2_000_000.0
    assert parse("1.5 milhões") == 1_500_000.0


def test_suffix_mi_short():
    assert parse("2,5 mi") == 2_500_000.0
    assert parse("3 mi") == 3_000_000.0


# --- casos degenerados -----------------------------------------------------

def test_none_input():
    assert parse(None) is None


def test_empty_string():
    assert parse("") is None
    assert parse("   ") is None


def test_non_numeric():
    assert parse("abc") is None
    assert parse("R$") is None


def test_zero_or_negative():
    assert parse("0") is None
    assert parse("-100") is None


def test_very_small_decimal():
    # "1.5" — sem grupo de 3 dígitos no final → decimal
    assert parse("1.5") == 1.5


def test_preserves_decimal_when_not_thousands():
    # "185.0" — grupo final com 1 dígito → decimal genuíno
    assert parse("185.0") == 185.0
