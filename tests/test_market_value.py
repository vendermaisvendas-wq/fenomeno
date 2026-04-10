"""Testes das funções puras de market_value (sem DB)."""

import pytest

from market_value import (
    PricedItem, compute_group_stats, find_comparables, percentile,
)


def _item(id_, price, title_tokens, brand=None, year=None):
    return PricedItem(
        id=id_, price=price, tokens=set(title_tokens),
        brand=brand, year=year, title=" ".join(title_tokens),
    )


def test_percentile_median_of_odd_list():
    assert percentile([1, 2, 3, 4, 5], 50) == 3


def test_percentile_quartiles():
    assert percentile([1, 2, 3, 4, 5, 6, 7, 8, 9], 25) == 3.0
    assert percentile([1, 2, 3, 4, 5, 6, 7, 8, 9], 75) == 7.0


def test_percentile_single_element():
    assert percentile([42.0], 50) == 42.0
    assert percentile([42.0], 25) == 42.0


def test_percentile_empty_raises():
    with pytest.raises(ValueError):
        percentile([], 50)


def test_compute_group_stats_basic():
    gs = compute_group_stats([100.0, 200.0, 300.0, 400.0, 500.0])
    assert gs is not None
    assert gs.count == 5
    assert gs.median == 300.0
    assert gs.mean == 300.0
    assert gs.p25 == 200.0
    assert gs.p75 == 400.0
    assert gs.stdev > 0


def test_compute_group_stats_too_few():
    assert compute_group_stats([100.0]) is None
    assert compute_group_stats([]) is None


def test_find_comparables_prefers_same_year_when_enough():
    # 4 toyota 2020 → same_year >= MIN_COMPARABLES (3), retorna só os do ano
    pool = [
        _item("a", 150000, ["hilux", "srv"], brand="toyota", year=2020),
        _item("b", 145000, ["hilux", "sr"],  brand="toyota", year=2020),
        _item("c", 160000, ["hilux", "srx"], brand="toyota", year=2020),
        _item("d", 155000, ["hilux"],         brand="toyota", year=2020),
        _item("e", 180000, ["hilux", "srv"], brand="toyota", year=2022),
        _item("f",  30000, ["civic"],         brand="honda",  year=2020),
    ]
    comps = find_comparables(pool[0], pool)
    comp_ids = {c.id for c in comps}
    assert comp_ids == {"b", "c", "d"}  # só os 2020, sem o 2022 nem o civic


def test_find_comparables_relaxes_year_when_few_same_year():
    # Só 1 mesmo ano → mas ≥3 da mesma marca → retorna todos da marca
    pool = [
        _item("a", 150000, ["hilux", "srv"], brand="toyota", year=2020),
        _item("b", 145000, ["hilux"],         brand="toyota", year=2020),
        _item("c", 180000, ["hilux", "srv"], brand="toyota", year=2022),
        _item("d", 120000, ["hilux", "sr"],  brand="toyota", year=2019),
        _item("e",  30000, ["civic"],         brand="honda",  year=2020),
    ]
    comps = find_comparables(pool[0], pool)
    comp_ids = {c.id for c in comps}
    # same_year tem só {b} (< 3), então cai para same_brand (b, c, d)
    # civic é outra marca, excluído
    assert comp_ids == {"b", "c", "d"}


def test_find_comparables_relaxes_to_brand_only():
    # só 2 do mesmo ano → abaixo do min, deve relaxar para mesma marca
    pool = [
        _item("a", 100000, ["hilux"], brand="toyota", year=2020),
        _item("b", 110000, ["hilux"], brand="toyota", year=2020),
        _item("c", 120000, ["hilux"], brand="toyota", year=2018),
        _item("d", 130000, ["hilux"], brand="toyota", year=2019),
    ]
    comps = find_comparables(pool[0], pool)
    assert len(comps) == 3  # todos os outros hilux


def test_find_comparables_falls_back_to_jaccard():
    # sem marca reconhecida, usa jaccard
    pool = [
        _item("a", 1000, ["bicicleta", "aro", "29", "alumínio"]),
        _item("b",  900, ["bicicleta", "aro", "29", "carbono"]),
        _item("c", 1200, ["bicicleta", "aro", "26"]),
        _item("d",  500, ["geladeira", "brastemp"]),
    ]
    comps = find_comparables(pool[0], pool)
    comp_ids = {c.id for c in comps}
    # b tem 3/5 = 0.6 em jaccard (>=0.5) — passa
    # c tem 2/5 = 0.4 — não passa
    # d tem 0 — não passa
    assert "b" in comp_ids
    assert "d" not in comp_ids


def test_find_comparables_excludes_self():
    pool = [
        _item("a", 100, ["hilux"], brand="toyota", year=2020),
        _item("b", 110, ["hilux"], brand="toyota", year=2020),
        _item("c", 120, ["hilux"], brand="toyota", year=2020),
    ]
    comps = find_comparables(pool[0], pool)
    assert all(c.id != "a" for c in comps)
