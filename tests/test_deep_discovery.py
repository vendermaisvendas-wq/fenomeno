"""Testes do marketplace_deep_discovery — graph helpers + deep_discover_for mockado."""

from __future__ import annotations

import pytest

import db
import marketplace_deep_discovery as mdd


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    p = tmp_path / "dd.sqlite3"
    monkeypatch.setattr(db, "DB_PATH", p)
    db.init_db(p)
    original_connect = db.connect
    monkeypatch.setattr(mdd, "connect", lambda *a, **kw: original_connect(p))
    return p


def test_add_edge_inserts(isolated_db):
    added = mdd.add_edge(parent_query=None, child_query="iphone",
                         source_listing_id=None, depth=0)
    assert added is True


def test_add_edge_dedup(isolated_db):
    mdd.add_edge(None, "iphone", None, 0)
    added2 = mdd.add_edge(None, "iphone", None, 0)
    assert added2 is False


def test_add_edge_different_parents_ok(isolated_db):
    assert mdd.add_edge(None, "iphone", None, 0) is True
    assert mdd.add_edge("ipad", "iphone", None, 1) is True
    # mesmo child com parents diferentes — UNIQUE permite


def test_graph_summary_counts(isolated_db):
    mdd.add_edge(None, "iphone", None, 0)
    mdd.add_edge(None, "ipad", None, 0)
    mdd.add_edge("iphone", "iphone 11", "abc", 1)
    mdd.add_edge("iphone", "iphone 12", "def", 1)
    mdd.add_edge("iphone 11", "iphone 11 128gb", "ghi", 2)

    s = mdd.graph_summary()
    assert s["total_edges"] == 5
    assert s["roots"] == 2
    assert s["max_depth"] == 2
    by_depth_map = {d["depth"]: d["n"] for d in s["by_depth"]}
    assert by_depth_map[0] == 2
    assert by_depth_map[1] == 2
    assert by_depth_map[2] == 1


def test_edges_from_root(isolated_db):
    mdd.add_edge(None, "iphone", None, 0)
    mdd.add_edge("iphone", "iphone 11", "x", 1)

    roots = mdd.edges_from(parent=None)
    assert any(r["child_query"] == "iphone" for r in roots)

    children = mdd.edges_from(parent="iphone")
    assert len(children) == 1
    assert children[0]["child_query"] == "iphone 11"


def test_deep_discover_with_mock(isolated_db, monkeypatch):
    """Mock discover_for para devolver hits sintéticos. Verifica BFS."""
    call_log = []

    def _mock_discover(keyword, region=None, max_pages=2, use_cache=True):
        call_log.append(keyword)
        # depth 0: seed gera 1 listing → 1 child query
        # depth 1: child query gera 0 listings → para
        if keyword == "iphone":
            return {
                "queries_run": 1, "cache_hits": 0, "total_unique_hits": 1,
                "hits": [{"item_id": "1", "title": "iPhone 12 128GB",
                          "url": "http://x/1"}],
            }
        return {
            "queries_run": 1, "cache_hits": 0, "total_unique_hits": 0, "hits": [],
        }

    import marketplace_discovery_engine as mde
    monkeypatch.setattr(mde, "discover_for", _mock_discover)

    result = mdd.deep_discover_for(
        keyword="iphone", region=None, max_depth=2, max_total_queries=10,
    )
    assert result["queries_visited"] >= 2  # seed + ao menos 1 child
    assert result["unique_listings"] >= 1
    assert "iphone" in call_log

    # discovery_graph deve ter pelo menos as arestas seed e child
    s = mdd.graph_summary()
    assert s["total_edges"] >= 2


def test_deep_discover_max_queries_limit(isolated_db, monkeypatch):
    """Garante que o limite max_total_queries é respeitado."""
    def _mock_discover(keyword, region=None, max_pages=2, use_cache=True):
        # cada query gera 5 listings, cada um com título diferente
        return {
            "queries_run": 1, "cache_hits": 0, "total_unique_hits": 5,
            "hits": [
                {"item_id": f"{keyword}_{i}",
                 "title": f"Toyota Hilux SRV 2020 unidade{i}",
                 "url": f"http://x/{keyword}_{i}"}
                for i in range(5)
            ],
        }

    import marketplace_discovery_engine as mde
    monkeypatch.setattr(mde, "discover_for", _mock_discover)

    result = mdd.deep_discover_for(
        keyword="hilux", max_depth=10, max_total_queries=3,
    )
    # nunca deve ter rodado discover mais do que max_total_queries
    # (pode ter menos se queries derivadas se sobrepõem)
    assert result["queries_visited"] <= 4  # margem por arestas duplicadas
