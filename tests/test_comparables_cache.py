"""Testes do cache persistente de ComparablesIndex."""

from __future__ import annotations

import pytest

import comparables_cache as cc
from market_value import PricedItem


def _make_items():
    return [
        PricedItem(id="a", price=100.0, tokens={"hilux"},
                   brand="toyota", year=2020, title="hilux"),
        PricedItem(id="b", price=150.0, tokens={"hilux"},
                   brand="toyota", year=2021, title="hilux"),
        PricedItem(id="c", price=80.0, tokens={"civic"},
                   brand="honda", year=2019, title="civic"),
    ]


def test_fingerprint_stable_across_runs():
    items = _make_items()
    assert cc._fingerprint_items(items) == cc._fingerprint_items(items)


def test_fingerprint_changes_with_price_change():
    items = _make_items()
    fp1 = cc._fingerprint_items(items)
    items[0].price = 105.0
    fp2 = cc._fingerprint_items(items)
    assert fp1 != fp2


def test_fingerprint_changes_with_new_item():
    items = _make_items()
    fp1 = cc._fingerprint_items(items)
    items.append(PricedItem(
        id="d", price=500.0, tokens={"pz"},
        brand=None, year=None, title="pz",
    ))
    fp2 = cc._fingerprint_items(items)
    assert fp1 != fp2


def test_fingerprint_invariant_to_order():
    items = _make_items()
    reversed_items = list(reversed(items))
    assert cc._fingerprint_items(items) == cc._fingerprint_items(reversed_items)


def test_save_and_load_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setattr(cc, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(cc, "CACHE_FILE", tmp_path / "comparables.pkl")
    items = _make_items()
    cc.save_cache(items)
    loaded, fp = cc.load_cache()
    assert loaded is not None
    assert len(loaded) == 3
    assert fp == cc._fingerprint_items(items)
    # Check that PricedItem fields roundtrip
    assert loaded[0].id == "a"
    assert loaded[0].brand == "toyota"


def test_load_cache_missing_returns_none(tmp_path, monkeypatch):
    monkeypatch.setattr(cc, "CACHE_FILE", tmp_path / "nope.pkl")
    items, fp = cc.load_cache()
    assert items is None
    assert fp is None


def test_invalidate_removes_file(tmp_path, monkeypatch):
    monkeypatch.setattr(cc, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(cc, "CACHE_FILE", tmp_path / "comparables.pkl")
    cc.save_cache(_make_items())
    assert cc.CACHE_FILE.exists()
    assert cc.invalidate() is True
    assert not cc.CACHE_FILE.exists()
    # Segunda chamada: nada para remover
    assert cc.invalidate() is False
