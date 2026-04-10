"""
Painel web (read-only) sobre o banco.

Rotas:
  GET /                      listagem principal
  GET /item/{id}             detalhe com histórico de preço (Chart.js)
  GET /explorer?q=           busca por substring no título
  GET /stats                 estatísticas gerais + distribuição de preços
  GET /opportunities         anúncios com flag de oportunidade
  GET /api/stats             JSON
  GET /api/price_history/{id}  JSON para Chart.js
  GET /api/price_distribution  histograma global (bins=20)

Execução:
    uvicorn web:app --reload --port 8000
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from fastapi import BackgroundTasks, FastAPI, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from analytics import _to_float
from db import (
    connect, events_for, listing_by_id, price_history_for, snapshots_for,
)

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app = FastAPI(title="FB Marketplace Audit")


def _t(name: str, ctx: dict):
    """Renderiza template Jinja2 diretamente, bypassing Starlette TemplateResponse
    que tem bug de cache com Jinja2 3.1.5+ (unhashable dict as cache key)."""
    template = templates.env.get_template(name)
    html = template.render(ctx)
    return HTMLResponse(content=html)


@app.on_event("startup")
def startup():
    from db import init_db
    init_db()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/debug-database")
def debug_database():
    """Estado bruto do banco."""
    with connect() as conn:
        listings = [dict(r) for r in conn.execute(
            "SELECT id, current_title, current_price, current_location, last_status "
            "FROM listings ORDER BY first_seen_at DESC LIMIT 20"
        ).fetchall()]
        wr = [dict(r) for r in conn.execute(
            "SELECT watch_id, listing_id, first_seen FROM watcher_results "
            "ORDER BY first_seen DESC LIMIT 20"
        ).fetchall()]
    return JSONResponse({"listings": listings, "watcher_results": wr})


@app.get("/debug-discovery", response_class=HTMLResponse)
def debug_discovery(request: Request,
                    keyword: str = Query("iphone"),
                    region: str = Query(""),
                    limit: int = Query(8, ge=1, le=20)):
    """Discovery via core_discovery: DDG → validação FB → persist."""
    from core_discovery import discover_and_validate
    from dataclasses import asdict
    result = discover_and_validate(
        keyword=keyword, region=region or None,
        max_validate=limit, persist=True,
    )
    r = asdict(result)
    html = """{% extends "base.html" %}
{% block title %}Testar Discovery{% endblock %}
{% block content %}
<h3>Testar Discovery</h3>
<p class="text-muted">DDG busca URLs → Facebook valida cada uma → insere ativos no banco.</p>
<form method="get" class="row g-2 mb-4">
  <div class="col-auto"><label class="form-label">Palavra-chave</label>
    <input type="text" name="keyword" value="{{ keyword }}" class="form-control"></div>
  <div class="col-auto"><label class="form-label">Regiao</label>
    <input type="text" name="region" value="{{ region }}" class="form-control" placeholder="opcional"></div>
  <div class="col-auto"><label class="form-label">Validar</label>
    <input type="number" name="limit" value="{{ limit }}" min="1" max="20" class="form-control" style="width:80px"></div>
  <div class="col-auto d-flex align-items-end">
    <button class="btn btn-primary" type="submit">Executar</button></div>
</form>
<div class="card mb-3"><div class="card-body">
  <strong>Queries:</strong> {{ r.queries_executed|join(' | ') }}<br>
  <strong>URLs no DDG:</strong> {{ r.urls_found }}<br>
  <strong>Validados no FB:</strong> {{ r.validated }}<br>
  <strong>Ativos:</strong> <span class="text-success fw-bold">{{ r.active }}</span> ·
  <strong>Rejeitados:</strong> {{ r.rejected }}<br>
  <strong>Novos inseridos no banco:</strong> <span class="text-primary fw-bold">{{ r.inserted }}</span>
  {% if r.errors %}<br><span class="text-danger">Erros: {{ r.errors|join('; ') }}</span>{% endif %}
</div></div>
{% if r.listings %}
<h5 class="text-success">Anuncios ATIVOS encontrados</h5>
<table class="table table-sm table-hover">
  <thead class="table-light"><tr><th>ID</th><th>Titulo</th><th>Preco</th><th>Local</th><th></th></tr></thead>
  <tbody>{% for v in r.listings %}
    <tr><td class="mono">{{ v.item_id }}</td><td>{{ v.title[:70] }}</td>
      <td class="price">{{ v.price }} {{ v.currency }}</td><td>{{ v.location }}</td>
      <td><a href="{{ v.url }}" target="_blank">FB</a></td></tr>
  {% endfor %}</tbody>
</table>{% endif %}
{% if r.rejected_list %}
<h5 class="text-muted">Filtrados (vendidos/removidos)</h5>
<table class="table table-sm text-muted">
  <thead><tr><th>ID</th><th>Motivo</th><th>Titulo DDG</th></tr></thead>
  <tbody>{% for x in r.rejected_list %}
    <tr><td class="mono">{{ x.item_id }}</td><td>{{ x.reason }}</td><td>{{ x.title_ddg }}</td></tr>
  {% endfor %}</tbody>
</table>{% endif %}
<a href="/" class="btn btn-outline-primary mt-3">Ver no dashboard</a>
{% endblock %}"""
    tpl = templates.env.from_string(html)
    return HTMLResponse(tpl.render(request=request, keyword=keyword,
                                   region=region, limit=limit, r=r))


@app.get("/debug-pipeline")
def debug_pipeline():
    """Diagnóstico JSON rápido de todos os componentes."""
    import traceback
    checks = {}
    # DB
    try:
        with connect() as conn:
            checks["banco"] = {
                "listings": conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0],
                "listings_ok": conn.execute("SELECT COUNT(*) FROM listings WHERE last_status='ok'").fetchone()[0],
                "watchers": conn.execute("SELECT COUNT(*) FROM watchers").fetchone()[0],
                "watcher_results": conn.execute("SELECT COUNT(*) FROM watcher_results").fetchone()[0],
                "events": conn.execute("SELECT COUNT(*) FROM events").fetchone()[0],
            }
    except Exception as e:
        checks["banco"] = f"ERRO: {e}"
    # Search backend
    import os
    serper_key = os.environ.get("SERPER_API_KEY")
    if serper_key:
        checks["search_backend"] = "Serper.dev (Google API)"
        try:
            from core_discovery import _serper_search
            r = _serper_search("facebook marketplace teste", serper_key, 3)
            checks["search_test"] = f"OK ({len(r)} resultados)"
        except Exception as e:
            checks["search_test"] = f"ERRO: {e}"
    else:
        checks["search_backend"] = "DDG (fallback — bloqueado em datacenters)"
        checks["SERPER_API_KEY"] = "NAO CONFIGURADA — configure em serper.dev"
        try:
            from ddgs import DDGS
            r = DDGS().text("facebook marketplace teste", max_results=3)
            checks["search_test"] = f"DDG: {len(r)} resultados"
        except Exception as e:
            checks["search_test"] = f"DDG BLOQUEADO: {e}"
    # Extract
    try:
        from extract_item import extract
        checks["extract"] = "modulo importado OK"
    except Exception as e:
        checks["extract"] = f"ERRO: {e}"
    # Templates
    try:
        n = len(list((Path(__file__).parent / "templates").glob("*.html")))
        checks["templates"] = f"OK ({n} arquivos)"
    except Exception as e:
        checks["templates"] = f"ERRO: {e}"
    return JSONResponse(checks)


@app.get("/debug-watchers")
def debug_watchers():
    """Status JSON de cada watcher."""
    with connect() as conn:
        watchers = conn.execute(
            """SELECT w.watch_id, w.keyword, w.region, w.is_active, w.last_run_at,
                  (SELECT COUNT(*) FROM watcher_results wr WHERE wr.watch_id = w.watch_id) as matches
             FROM watchers w ORDER BY w.watch_id"""
        ).fetchall()
        result = []
        for w in watchers:
            last_matches = conn.execute(
                """SELECT wr.listing_id, l.current_title, l.current_price
                  FROM watcher_results wr
                  LEFT JOIN listings l ON l.id = wr.listing_id
                  WHERE wr.watch_id = ? ORDER BY wr.first_seen DESC LIMIT 5""",
                (w["watch_id"],)
            ).fetchall()
            result.append({
                "watch_id": w["watch_id"],
                "keyword": w["keyword"],
                "region": w["region"],
                "active": bool(w["is_active"]),
                "last_run": w["last_run_at"],
                "total_matches": w["matches"],
                "recent_matches": [
                    {"id": m["listing_id"], "title": m["current_title"],
                     "price": m["current_price"]}
                    for m in last_matches
                ],
            })
    return JSONResponse(result)


@app.get("/system-status", response_class=HTMLResponse)
def system_status_page(request: Request):
    with connect() as conn:
        listings_total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
        listings_active = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE is_removed = 0"
        ).fetchone()[0]
        watchers_total = conn.execute("SELECT COUNT(*) FROM watchers").fetchone()[0]
        watchers_active = conn.execute(
            "SELECT COUNT(*) FROM watchers WHERE is_active = 1"
        ).fetchone()[0]
        events_total = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        alerts_total = conn.execute(
            "SELECT COUNT(*) FROM events WHERE event_type = 'alert_sent'"
        ).fetchone()[0]
        watcher_matches = conn.execute(
            "SELECT COUNT(*) FROM events WHERE event_type = 'watcher_match'"
        ).fetchone()[0]
        last_event = conn.execute(
            "SELECT at FROM events ORDER BY at DESC LIMIT 1"
        ).fetchone()
        last_event_at = last_event["at"] if last_event else "nenhum"
        watcher_results = conn.execute(
            "SELECT COUNT(*) FROM watcher_results"
        ).fetchone()[0]
        snapshots = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]

    html = f"""
    {{% extends "base.html" %}}
    {{% block title %}}Status do Sistema{{% endblock %}}
    {{% block content %}}
    <h3>Status do Sistema</h3>
    <div class="row g-3 mb-4">
      <div class="col-md-3"><div class="card metric-card"><div class="card-body">
        <div class="label">Anúncios</div><h2>{listings_total}</h2>
        <small class="text-muted">{listings_active} ativos</small>
      </div></div></div>
      <div class="col-md-3"><div class="card metric-card"><div class="card-body">
        <div class="label">Monitoramentos</div><h2>{watchers_total}</h2>
        <small class="text-muted">{watchers_active} ativos</small>
      </div></div></div>
      <div class="col-md-3"><div class="card metric-card"><div class="card-body">
        <div class="label">Alertas enviados</div><h2>{alerts_total}</h2>
        <small class="text-muted">{watcher_matches} matches de watcher</small>
      </div></div></div>
      <div class="col-md-3"><div class="card metric-card"><div class="card-body">
        <div class="label">Último evento</div>
        <h2 style="font-size:1rem">{last_event_at[:19] if len(last_event_at) > 19 else last_event_at}</h2>
      </div></div></div>
    </div>
    <div class="card">
      <div class="card-header">Detalhes</div>
      <table class="table table-sm mb-0">
        <tr><td>Anúncios no banco</td><td class="text-end"><strong>{listings_total}</strong></td></tr>
        <tr><td>Anúncios ativos</td><td class="text-end">{listings_active}</td></tr>
        <tr><td>Snapshots</td><td class="text-end">{snapshots}</td></tr>
        <tr><td>Eventos totais</td><td class="text-end">{events_total}</td></tr>
        <tr><td>Matches de watcher</td><td class="text-end">{watcher_matches}</td></tr>
        <tr><td>Alertas enviados</td><td class="text-end">{alerts_total}</td></tr>
        <tr><td>Resultados de watcher</td><td class="text-end">{watcher_results}</td></tr>
        <tr><td>Monitoramentos total</td><td class="text-end">{watchers_total}</td></tr>
        <tr><td>Monitoramentos ativos</td><td class="text-end">{watchers_active}</td></tr>
      </table>
    </div>
    {{% endblock %}}
    """
    # Render inline template (sem arquivo separado)
    from jinja2 import Environment
    env = templates.env
    tpl = env.from_string(html)
    return HTMLResponse(tpl.render(request=request))


# --- páginas HTML ----------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT id, url, current_title, current_price, current_currency,
                   current_location, last_status, last_seen_at, is_removed
              FROM listings
             ORDER BY last_seen_at DESC
             LIMIT 500
            """
        ).fetchall()
        total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
        removed = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE is_removed = 1"
        ).fetchone()[0]
    return _t(
        "index.html",
        {"request": request, "rows": rows, "total": total, "removed": removed},
    )


@app.get("/item/{item_id}", response_class=HTMLResponse)
def item_detail(item_id: str, request: Request):
    with connect() as conn:
        listing = listing_by_id(conn, item_id)
        if listing is None:
            return HTMLResponse(f"<h3>not found: {item_id}</h3>", status_code=404)
        evts = events_for(conn, item_id)
        snaps = snapshots_for(conn, item_id, limit=20)
        ph = price_history_for(conn, item_id)

    last_payload = None
    for s in snaps:
        try:
            last_payload = json.loads(s["payload_json"])
            break
        except Exception:
            continue

    return _t(
        "item.html",
        {
            "request": request,
            "listing": listing,
            "events": evts,
            "snapshots": snaps,
            "payload": last_payload,
            "price_history": [
                {"t": row["recorded_at"], "price": row["price"]}
                for row in ph
            ],
        },
    )


@app.get("/explorer", response_class=HTMLResponse)
def explorer(request: Request, q: str = Query("")):
    q_clean = q.strip().lower()
    with connect() as conn:
        if q_clean:
            rows = conn.execute(
                """
                SELECT id, url, current_title, current_price, current_currency,
                       last_status, last_seen_at, is_removed
                  FROM listings
                 WHERE LOWER(COALESCE(current_title, '')) LIKE ?
                 ORDER BY last_seen_at DESC
                 LIMIT 500
                """,
                (f"%{q_clean}%",),
            ).fetchall()
        else:
            rows = []
    return _t(
        "explorer.html", {"request": request, "rows": rows, "q": q},
    )


@app.get("/stats", response_class=HTMLResponse)
def stats_page(request: Request):
    with connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
        active = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE is_removed = 0"
        ).fetchone()[0]
        removed = total - active
        events_total = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        snapshots_total = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        ph_total = conn.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]

        status_rows = conn.execute(
            "SELECT last_status, COUNT(*) AS n FROM listings GROUP BY last_status"
        ).fetchall()
        source_rows = conn.execute(
            "SELECT COALESCE(source, '(null)') AS source, COUNT(*) AS n "
            "FROM listings GROUP BY source"
        ).fetchall()
        event_type_rows = conn.execute(
            "SELECT event_type, COUNT(*) AS n FROM events "
            "GROUP BY event_type ORDER BY n DESC"
        ).fetchall()
        flagged_listings = conn.execute(
            "SELECT COUNT(DISTINCT listing_id) FROM events "
            "WHERE event_type = 'opportunity_flag'"
        ).fetchone()[0]

    return _t(
        "stats.html",
        {
            "request": request,
            "total": total,
            "active": active,
            "removed": removed,
            "events_total": events_total,
            "snapshots_total": snapshots_total,
            "price_history_total": ph_total,
            "flagged_listings": flagged_listings,
            "statuses": status_rows,
            "sources": source_rows,
            "event_types": event_type_rows,
        },
    )


@app.get("/opportunities", response_class=HTMLResponse)
def opportunities_page(request: Request, rule: str = Query("")):
    where_rule = ""
    params: tuple = ()
    if rule:
        where_rule = "AND e.new_value LIKE ?"
        params = (f"{rule}:%",)

    with connect() as conn:
        rows = conn.execute(
            f"""
            SELECT l.id, l.url, l.current_title, l.current_price, l.current_currency,
                   l.last_status, l.is_removed,
                   e.new_value AS flag, e.at AS flagged_at
              FROM events e
              JOIN listings l ON l.id = e.listing_id
             WHERE e.event_type = 'opportunity_flag'
               {where_rule}
             ORDER BY e.at DESC
             LIMIT 500
            """,
            params,
        ).fetchall()
        rule_counts = conn.execute(
            """
            SELECT
              SUBSTR(new_value, 1, INSTR(new_value, ':') - 1) AS rule,
              COUNT(*) AS n
            FROM events
            WHERE event_type = 'opportunity_flag'
              AND INSTR(new_value, ':') > 0
            GROUP BY rule
            ORDER BY n DESC
            """
        ).fetchall()
    return _t(
        "opportunities.html",
        {"request": request, "rows": rows, "rule_counts": rule_counts, "rule": rule},
    )


# --- APIs JSON -------------------------------------------------------------

@app.get("/api/stats")
def api_stats():
    with connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
        active = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE is_removed = 0"
        ).fetchone()[0]
        events_total = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        snapshots_total = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        ph_total = conn.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]
    return JSONResponse({
        "listings_total": total,
        "listings_active": active,
        "events_total": events_total,
        "snapshots_total": snapshots_total,
        "price_history_total": ph_total,
    })


@app.get("/api/price_history/{item_id}")
def api_price_history(item_id: str):
    with connect() as conn:
        rows = price_history_for(conn, item_id)
    return JSONResponse({
        "labels": [r["recorded_at"] for r in rows],
        "prices": [r["price"] for r in rows],
        "currencies": list({r["currency"] for r in rows if r["currency"]}),
    })


@app.get("/top-deals", response_class=HTMLResponse)
def top_deals(request: Request, limit: int = Query(50, ge=1, le=200)):
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT id, url, current_title, current_price, current_currency,
                   current_location, last_status, is_removed,
                   estimated_market_value, discount_percentage, opportunity_score,
                   cluster_id, last_seen_at
              FROM listings
             WHERE is_removed = 0
               AND opportunity_score IS NOT NULL
             ORDER BY opportunity_score DESC, discount_percentage DESC
             LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return _t(
        "top_deals.html",
        {"request": request, "rows": rows, "limit": limit},
    )


@app.get("/api/price_heatmap")
def api_price_heatmap(min_count: int = Query(5, ge=2)):
    """Top tokens por contagem: para cada um retorna mean, median, p25, p75, n."""
    from market_value import token_group_stats
    stats = token_group_stats(min_count=min_count)
    items = sorted(
        (
            {
                "token": tok,
                "count": gs.count,
                "mean": round(gs.mean, 2),
                "median": round(gs.median, 2),
                "p25": round(gs.p25, 2),
                "p75": round(gs.p75, 2),
                "stdev": round(gs.stdev, 2),
            }
            for tok, gs in stats.items()
        ),
        key=lambda x: -x["count"],
    )
    return JSONResponse({"groups": items[:40]})


@app.get("/market-insights", response_class=HTMLResponse)
def market_insights(request: Request):
    from sales_velocity import compute_by_token, compute_global
    with connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
        active = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE is_removed = 0"
        ).fetchone()[0]
        outliers = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE price_outlier = 1"
        ).fetchone()[0]
        high_fraud = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE fraud_risk_score >= 50"
        ).fetchone()[0]
        clustered = conn.execute(
            "SELECT COUNT(DISTINCT cluster_id) FROM listings "
            "WHERE cluster_id IS NOT NULL"
        ).fetchone()[0]
        avg_score = conn.execute(
            "SELECT AVG(opportunity_score) FROM listings "
            "WHERE opportunity_score IS NOT NULL"
        ).fetchone()[0]
    global_velocity = compute_global()
    top_tokens = compute_by_token(limit=15)

    return _t(
        "market_insights.html",
        {
            "request": request,
            "total": total, "active": active, "outliers": outliers,
            "high_fraud": high_fraud, "clustered": clustered,
            "avg_score": round(avg_score or 0, 1),
            "velocity_global": global_velocity,
            "velocity_tokens": top_tokens,
        },
    )


@app.get("/price-trends", response_class=HTMLResponse)
def price_trends(request: Request):
    return _t(
        "price_trends.html", {"request": request}
    )


@app.get("/liquidity", response_class=HTMLResponse)
def liquidity_page(request: Request, limit: int = Query(100, ge=1, le=500)):
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT id, url, current_title, current_price, current_currency,
                   current_location, opportunity_score, liquidity_score,
                   discount_percentage, cluster_id
              FROM listings
             WHERE is_removed = 0 AND liquidity_score IS NOT NULL
             ORDER BY liquidity_score DESC, opportunity_score DESC
             LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return _t(
        "liquidity.html", {"request": request, "rows": rows},
    )


@app.get("/predicted-price", response_class=HTMLResponse)
def predicted_price_page(request: Request, limit: int = Query(100, ge=1, le=500)):
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT id, url, current_title, current_price, current_currency,
                   estimated_market_value, predicted_price, price_gap,
                   opportunity_score
              FROM listings
             WHERE is_removed = 0
               AND predicted_price IS NOT NULL
               AND price_gap IS NOT NULL
             ORDER BY price_gap DESC
             LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return _t(
        "predicted_price.html", {"request": request, "rows": rows},
    )


@app.get("/sellers", response_class=HTMLResponse)
def sellers_page(request: Request, limit: int = Query(50, ge=1, le=500)):
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT seller_name, total_listings, active_listings, removed_listings,
                   duplicate_count, avg_price, avg_opportunity, reliability_score,
                   computed_at
              FROM seller_stats
             ORDER BY total_listings DESC
             LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return _t(
        "sellers.html", {"request": request, "rows": rows},
    )


@app.get("/outliers", response_class=HTMLResponse)
def outliers_page(request: Request):
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT id, url, current_title, current_price, current_currency,
                   current_location, estimated_market_value,
                   discount_percentage, opportunity_score,
                   fraud_risk_score, last_seen_at
              FROM listings
             WHERE price_outlier = 1
               AND is_removed = 0
             ORDER BY ABS(COALESCE(discount_percentage, 0)) DESC
             LIMIT 200
            """
        ).fetchall()
    return _t(
        "outliers.html", {"request": request, "rows": rows},
    )


@app.get("/api/price_trends")
def api_price_trends(keyword: str = Query("", description="substring no título"),
                     days: int = Query(30, ge=1, le=365)):
    """Série temporal: mediana de preço por dia, opcionalmente filtrada por keyword."""
    params: list = [days]
    where_keyword = ""
    if keyword.strip():
        where_keyword = (
            "AND l.id IN (SELECT id FROM listings "
            "WHERE LOWER(COALESCE(current_title, '')) LIKE ?) "
        )
        params.append(f"%{keyword.strip().lower()}%")

    with connect() as conn:
        rows = conn.execute(
            f"""
            SELECT SUBSTR(ph.recorded_at, 1, 10) AS day,
                   ph.price
              FROM price_history ph
              JOIN listings l ON l.id = ph.listing_id
             WHERE ph.recorded_at >= datetime('now', '-' || ? || ' days')
               {where_keyword}
             ORDER BY day ASC
            """,
            params,
        ).fetchall()

    buckets: dict[str, list[float]] = {}
    for r in rows:
        buckets.setdefault(r["day"], []).append(r["price"])

    import statistics as _st
    days_sorted = sorted(buckets)
    return JSONResponse({
        "labels": days_sorted,
        "median": [round(_st.median(buckets[d]), 2) for d in days_sorted],
        "count":  [len(buckets[d]) for d in days_sorted],
    })


@app.get("/geo-insights", response_class=HTMLResponse)
def geo_insights_page(request: Request):
    from geo_heatmap import by_state, top_cities_by_discount, top_cities_by_volume
    top_vol = top_cities_by_volume(25)
    top_disc = top_cities_by_discount(25)
    states = by_state()
    return _t(
        "geo_insights.html",
        {
            "request": request,
            "top_volume": top_vol,
            "top_discount": top_disc,
            "states": states,
        },
    )


@app.get("/anuncio-timeline/{listing_id}", response_class=HTMLResponse)
def listing_timeline_page(listing_id: str, request: Request):
    from listing_timeline import build_timeline
    entries = build_timeline(listing_id)
    with connect() as conn:
        listing = listing_by_id(conn, listing_id)
    return _t(
        "listing_timeline.html",
        {"request": request, "entries": entries, "listing": listing,
         "listing_id": listing_id},
    )


@app.get("/fresh-deals", response_class=HTMLResponse)
def fresh_deals_page(request: Request, limit: int = Query(100, ge=1, le=500)):
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT id, url, current_title, current_price, current_currency,
                   current_location, discount_percentage, opportunity_score,
                   liquidity_score, fresh_opportunity_score, first_seen_at,
                   category
              FROM listings
             WHERE is_removed = 0
               AND fresh_opportunity_score IS NOT NULL
               AND fresh_opportunity_score > 0
             ORDER BY fresh_opportunity_score DESC, first_seen_at DESC
             LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return _t(
        "fresh_deals.html", {"request": request, "rows": rows},
    )


@app.get("/market-density", response_class=HTMLResponse)
def market_density_page(request: Request, limit: int = Query(100, ge=1, le=500)):
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT token, total_listings, active_listings, removed_listings,
                   removal_rate, avg_velocity_days, competition_score
              FROM market_density
             ORDER BY competition_score DESC
             LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return _t(
        "market_density.html", {"request": request, "rows": rows},
    )


@app.get("/watchers", response_class=HTMLResponse)
def watchers_page(request: Request):
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT w.watch_id, w.keyword, w.region, w.min_price, w.max_price,
                   w.is_active, w.last_run_at, w.created_at,
                   (SELECT COUNT(*) FROM watcher_results wr
                     WHERE wr.watch_id = w.watch_id) AS total_matches,
                   (SELECT COUNT(*) FROM watcher_results wr
                     WHERE wr.watch_id = w.watch_id AND is_initial_backfill = 0)
                     AS new_matches
              FROM watchers w
             ORDER BY w.is_active DESC, w.watch_id DESC
            """
        ).fetchall()
    return _t(
        "watchers.html", {"request": request, "rows": rows},
    )


@app.post("/watchers")
def watchers_create(
    background_tasks: BackgroundTasks,
    keyword: str = Form(...),
    region: str = Form(""),
    min_price: str = Form(""),
    max_price: str = Form(""),
    backfill: str = Form(""),
):
    from watcher_engine import create_watch, run_backfill

    def _to_float(s: str) -> float | None:
        s = s.strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None

    wid = create_watch(
        keyword=keyword.strip(),
        region=region.strip() or None,
        min_price=_to_float(min_price),
        max_price=_to_float(max_price),
    )
    if backfill == "1":
        try:
            result = run_backfill(wid)
            print(f"[watcher] backfill #{wid}: {result}")
        except Exception as e:
            print(f"[watcher] backfill #{wid} erro: {e}")
    return RedirectResponse(url=f"/watchers/{wid}", status_code=303)


@app.post("/watchers/{watch_id}/toggle")
def watchers_toggle(watch_id: int):
    with connect() as conn:
        row = conn.execute(
            "SELECT is_active FROM watchers WHERE watch_id = ?", (watch_id,)
        ).fetchone()
        if row is None:
            return RedirectResponse(url="/watchers", status_code=303)
        new_state = 0 if row["is_active"] else 1
        conn.execute(
            "UPDATE watchers SET is_active = ? WHERE watch_id = ?",
            (new_state, watch_id),
        )
    return RedirectResponse(url="/watchers", status_code=303)


@app.post("/watchers/{watch_id}/backfill")
def watchers_backfill(watch_id: int):
    from watcher_engine import run_backfill
    try:
        run_backfill(watch_id)
    except Exception as e:
        print(f"[watcher] backfill #{watch_id} erro: {e}")
    return RedirectResponse(url=f"/watchers/{watch_id}", status_code=303)


@app.post("/watchers/{watch_id}/monitor")
def watchers_monitor_now(watch_id: int):
    from watcher_engine import monitor_watch
    try:
        monitor_watch(watch_id)
    except Exception as e:
        print(f"[watcher] monitor #{watch_id} erro: {e}")
    return RedirectResponse(url=f"/watchers/{watch_id}", status_code=303)


@app.post("/watchers/{watch_id}/delete")
def watchers_delete(watch_id: int):
    with connect() as conn:
        conn.execute("DELETE FROM watcher_results WHERE watch_id = ?", (watch_id,))
        conn.execute("DELETE FROM watchers WHERE watch_id = ?", (watch_id,))
    return RedirectResponse(url="/watchers", status_code=303)


@app.get("/watchers/{watch_id}", response_class=HTMLResponse)
def watcher_detail(watch_id: int, request: Request):
    with connect() as conn:
        watcher = conn.execute(
            "SELECT * FROM watchers WHERE watch_id = ?", (watch_id,)
        ).fetchone()
        if watcher is None:
            return HTMLResponse(f"<h3>watcher {watch_id} not found</h3>", status_code=404)

        results = conn.execute(
            """
            SELECT wr.listing_id, wr.first_seen, wr.is_initial_backfill,
                   l.current_title, l.current_price, l.current_currency,
                   l.current_location, l.url, l.last_status, l.is_removed,
                   l.opportunity_score, l.discount_percentage
              FROM watcher_results wr
              LEFT JOIN listings l ON l.id = wr.listing_id
             WHERE wr.watch_id = ?
             ORDER BY wr.is_initial_backfill ASC, wr.first_seen DESC
             LIMIT 500
            """,
            (watch_id,),
        ).fetchall()
    return _t(
        "watcher_detail.html",
        {"request": request, "watcher": watcher, "results": results},
    )


@app.get("/watcher-insights", response_class=HTMLResponse)
def watcher_insights_page(request: Request):
    with connect() as conn:
        # Watchers mais ativos: maior total_matches
        top_watchers = conn.execute(
            """
            SELECT w.watch_id, w.keyword, w.region, w.priority, w.is_active,
                   w.last_run_at,
                   COUNT(wr.id) AS total_matches,
                   SUM(CASE WHEN wr.is_initial_backfill = 0 THEN 1 ELSE 0 END) AS new_matches
              FROM watchers w
              LEFT JOIN watcher_results wr ON wr.watch_id = w.watch_id
             GROUP BY w.watch_id
             ORDER BY total_matches DESC
             LIMIT 30
            """
        ).fetchall()

        # Distribuição por priority
        by_priority = conn.execute(
            """
            SELECT priority, COUNT(*) AS n
              FROM watchers WHERE is_active = 1
             GROUP BY priority ORDER BY priority
            """
        ).fetchall()

        # Tempo first_seen → alert (proxy interno)
        from product_metrics import time_to_alert_distribution
        time_to_alert = time_to_alert_distribution(days=30)

        # Grupos populares
        from watcher_optimizer import find_popular_groups
        popular = find_popular_groups(min_users=2)

    return _t(
        "watcher_insights.html",
        {
            "request": request,
            "top_watchers": top_watchers,
            "by_priority": by_priority,
            "time_to_alert": time_to_alert,
            "popular_groups": popular,
        },
    )


@app.get("/discovery-stats", response_class=HTMLResponse)
def discovery_stats_page(request: Request):
    from discovery_stats import build_report
    report = build_report(top=25, days=14)
    return _t(
        "discovery_stats.html",
        {"request": request, "report": report},
    )


@app.get("/api/discovery_stats")
def api_discovery_stats(top: int = Query(20, ge=1, le=100),
                        days: int = Query(7, ge=1, le=90)):
    from discovery_stats import build_report
    from dataclasses import asdict
    return JSONResponse(asdict(build_report(top=top, days=days)))


@app.get("/top-opportunities", response_class=HTMLResponse)
def top_opportunities_page(request: Request, limit: int = Query(100, ge=1, le=500)):
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT id, url, current_title, current_price, current_currency,
                   current_location, opportunity_probability, opportunity_score,
                   discount_percentage, liquidity_score, fresh_opportunity_score,
                   category, very_recent_listing
              FROM listings
             WHERE is_removed = 0
               AND opportunity_probability IS NOT NULL
             ORDER BY opportunity_probability DESC, opportunity_score DESC
             LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return _t(
        "top_opportunities.html", {"request": request, "rows": rows},
    )


@app.get("/discovery-network", response_class=HTMLResponse)
def discovery_network_page(request: Request):
    from marketplace_deep_discovery import edges_from, graph_summary
    summary = graph_summary()
    roots = edges_from(parent=None, limit=30)
    expansions = []
    for root in roots:
        children = edges_from(parent=root["child_query"], limit=10)
        expansions.append({"root": root, "children": children})
    return _t(
        "discovery_network.html",
        {"request": request, "summary": summary, "expansions": expansions},
    )


@app.get("/watchers-performance", response_class=HTMLResponse)
def watchers_performance_page(request: Request):
    from watcher_scheduler import debug as scheduler_debug
    rows = scheduler_debug()
    return _t(
        "watchers_performance.html",
        {"request": request, "rows": rows},
    )


@app.get("/api/listing_timeline/{listing_id}")
def api_listing_timeline(listing_id: str):
    from listing_timeline import build_timeline
    from dataclasses import asdict
    entries = build_timeline(listing_id)
    return JSONResponse({"entries": [asdict(e) for e in entries]})


@app.get("/api/price_distribution")
def api_price_distribution(bins: int = 20):
    with connect() as conn:
        rows = conn.execute(
            "SELECT current_price FROM listings "
            "WHERE is_removed = 0 AND current_price IS NOT NULL"
        ).fetchall()
    prices = [p for p in (_to_float(r["current_price"]) for r in rows) if p]
    if not prices:
        return JSONResponse({"labels": [], "counts": []})

    lo, hi = min(prices), max(prices)
    if lo == hi:
        return JSONResponse({"labels": [f"{lo:.0f}"], "counts": [len(prices)]})
    step = (hi - lo) / bins
    counts = [0] * bins
    for p in prices:
        idx = min(int((p - lo) / step), bins - 1)
        counts[idx] += 1
    labels = [f"{lo + i * step:.0f}" for i in range(bins)]
    return JSONResponse({"labels": labels, "counts": counts})
