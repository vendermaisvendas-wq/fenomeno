"""
Relatório HTML focado em inteligência de mercado.

Diferente de `weekly_report.py` (que é um diário das últimas mudanças),
este relatório consolida a visão MACRO: o que aconteceu com o mercado
como um todo no período.

Seções:
    1. Visão geral (cards)
    2. Top cidades por desconto médio (de geo_coverage)
    3. Categorias mais líquidas (de category_models stats)
    4. Tokens com maior competição (de market_density)
    5. Tokens com maior desconto médio (derivado)
    6. Fresh opportunities detectadas no período (eventos fresh_opportunity)
    7. Reposts detectados no período (eventos repost_detected)

Output: reports/market_report_YYYY-MM-DD.html + reports/market_latest.html

Uso:
    python market_report.py              # última semana
    python market_report.py --days 30
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from category_models import category_stats
from db import connect, init_db
from geo_heatmap import top_cities_by_discount, top_cities_by_volume
from logging_setup import get_logger, kv

log = get_logger("market_report")

REPORTS_DIR = Path("reports")


@dataclass
class MarketReport:
    window_start: str
    window_end: str
    days: int
    total_active: int
    total_cities: int
    total_categories: int
    total_tokens_tracked: int
    top_cities_volume: list = field(default_factory=list)
    top_cities_discount: list = field(default_factory=list)
    categories: list = field(default_factory=list)
    top_competition: list = field(default_factory=list)
    top_discount_tokens: list = field(default_factory=list)
    fresh_events: list = field(default_factory=list)
    repost_events: list = field(default_factory=list)


def _window(days: int) -> tuple[str, str]:
    now = datetime.now(timezone.utc)
    return (now - timedelta(days=days)).isoformat(timespec="seconds"), \
           now.isoformat(timespec="seconds")


def collect(days: int = 7) -> MarketReport:
    init_db()
    start, end = _window(days)

    with connect() as conn:
        active = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE is_removed = 0"
        ).fetchone()[0]
        n_cities = conn.execute("SELECT COUNT(*) FROM geo_coverage").fetchone()[0]
        n_tokens = conn.execute("SELECT COUNT(*) FROM market_density").fetchone()[0]

        # Top tokens de competição
        top_comp = [dict(r) for r in conn.execute(
            """
            SELECT token, active_listings, removal_rate, avg_velocity_days,
                   competition_score
              FROM market_density
             ORDER BY competition_score DESC
             LIMIT 20
            """
        ).fetchall()]

        # Top tokens por desconto médio (juntando market_density com discount dos listings)
        top_disc = [dict(r) for r in conn.execute(
            """
            SELECT md.token, md.active_listings, md.competition_score
              FROM market_density md
             ORDER BY md.active_listings DESC
             LIMIT 40
            """
        ).fetchall()]
        # Para cada um, calcular avg discount na hora (query filtrada por LIKE no título)
        for row in top_disc:
            tok = row["token"]
            r = conn.execute(
                """
                SELECT AVG(discount_percentage) FROM listings
                 WHERE discount_percentage IS NOT NULL
                   AND LOWER(COALESCE(current_title, '')) LIKE ?
                """,
                (f"%{tok}%",),
            ).fetchone()
            row["avg_discount"] = round(r[0], 1) if r[0] is not None else None
        top_disc = [r for r in top_disc if r["avg_discount"] is not None]
        top_disc.sort(key=lambda x: -x["avg_discount"])
        top_discount_tokens = top_disc[:15]

        fresh_events = [dict(r) for r in conn.execute(
            """
            SELECT e.at, l.current_title, l.current_price, l.url, e.new_value
              FROM events e
              JOIN listings l ON l.id = e.listing_id
             WHERE e.event_type = 'fresh_opportunity' AND e.at >= ?
             ORDER BY e.at DESC LIMIT 20
            """,
            (start,),
        ).fetchall()]

        repost_events = [dict(r) for r in conn.execute(
            """
            SELECT e.at, l.current_title, l.current_price, l.url,
                   l.current_seller, e.new_value
              FROM events e
              JOIN listings l ON l.id = e.listing_id
             WHERE e.event_type = 'repost_detected' AND e.at >= ?
             ORDER BY e.at DESC LIMIT 20
            """,
            (start,),
        ).fetchall()]

    cats = category_stats()
    vol = top_cities_by_volume(15)
    disc_cities = top_cities_by_discount(15)

    return MarketReport(
        window_start=start, window_end=end, days=days,
        total_active=active,
        total_cities=n_cities,
        total_categories=len(cats),
        total_tokens_tracked=n_tokens,
        top_cities_volume=[
            {"city": c.city, "state": c.state, "active": c.active_count,
             "avg_price": c.avg_price}
            for c in vol
        ],
        top_cities_discount=[
            {"city": c.city, "state": c.state, "avg_discount": c.avg_discount,
             "active": c.active_count}
            for c in disc_cities
        ],
        categories=[
            {"category": s.category, "active": s.active,
             "avg_price": s.avg_price, "avg_discount": s.avg_discount,
             "avg_liquidity": s.avg_liquidity}
            for s in cats
        ],
        top_competition=top_comp,
        top_discount_tokens=top_discount_tokens,
        fresh_events=fresh_events,
        repost_events=repost_events,
    )


# --- HTML rendering --------------------------------------------------------

HTML_TEMPLATE = """<!doctype html>
<html lang="pt-br">
<head>
<meta charset="utf-8">
<title>Market report — {window_end}</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css">
<style>
body {{ padding: 30px; max-width: 1200px; margin: 0 auto; }}
.metric {{ padding: 15px; background: #f8f9fa; border-radius: 6px; }}
.metric h2 {{ margin: 0; }}
.metric .label {{ font-size: 0.75rem; text-transform: uppercase;
                   color: #6c757d; letter-spacing: 0.05em; }}
h3 {{ margin-top: 2rem; border-bottom: 2px solid #dee2e6; padding-bottom: 0.5rem; }}
.mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.85rem; }}
</style>
</head>
<body>

<h1>Relatório de mercado — FB Marketplace Audit</h1>
<p class="text-muted">Janela: {window_start} → {window_end} ({days} dias)</p>

<div class="row g-3 mb-4">
  <div class="col"><div class="metric"><div class="label">ativos</div><h2>{total_active}</h2></div></div>
  <div class="col"><div class="metric"><div class="label">cidades cobertas</div><h2>{total_cities}</h2></div></div>
  <div class="col"><div class="metric"><div class="label">categorias</div><h2>{total_categories}</h2></div></div>
  <div class="col"><div class="metric"><div class="label">tokens rastreados</div><h2>{total_tokens_tracked}</h2></div></div>
</div>

<h3>Top cidades por volume</h3>
{top_cities_volume_table}

<h3>Top cidades por desconto médio</h3>
{top_cities_discount_table}

<h3>Categorias</h3>
{categories_table}

<h3>Tokens com maior competição</h3>
{top_competition_table}

<h3>Tokens com maior desconto médio</h3>
{top_discount_tokens_table}

<h3>Fresh opportunities detectadas no período</h3>
{fresh_events_table}

<h3>Reposts detectados no período</h3>
{repost_events_table}

<hr>
<p class="text-muted small">Gerado em {generated_at}</p>
</body>
</html>
"""


def _table(headers, rows):
    if not rows:
        return '<p class="text-muted">(nenhum)</p>'
    import html as _h
    out = '<table class="table table-sm table-striped"><thead><tr>'
    for col in headers:
        out += f"<th>{_h.escape(str(col))}</th>"
    out += "</tr></thead><tbody>"
    for row in rows:
        out += "<tr>"
        for cell in row:
            out += f"<td>{cell if cell is not None else ''}</td>"
        out += "</tr>"
    out += "</tbody></table>"
    return out


def render(r: MarketReport) -> str:
    import html as _h

    vol_rows = [[
        _h.escape(c["city"] or ""),
        c.get("state") or "-",
        c["active"],
        f"{c['avg_price']:,.0f}" if c.get("avg_price") else "-",
    ] for c in r.top_cities_volume]

    disc_city_rows = [[
        _h.escape(c["city"] or ""),
        c.get("state") or "-",
        f"{c['avg_discount']:.1f}%" if c.get("avg_discount") is not None else "-",
        c["active"],
    ] for c in r.top_cities_discount]

    cat_rows = [[
        c["category"],
        c["active"],
        f"{c['avg_price']:,.0f}" if c.get("avg_price") else "-",
        f"{c['avg_discount']:.1f}%" if c.get("avg_discount") is not None else "-",
        f"{c['avg_liquidity']:.1f}" if c.get("avg_liquidity") is not None else "-",
    ] for c in r.categories]

    comp_rows = [[
        c["token"],
        c["active_listings"],
        f"{c['removal_rate']:.2f}",
        f"{c['avg_velocity_days']:.1f}" if c.get("avg_velocity_days") is not None else "-",
        c["competition_score"],
    ] for c in r.top_competition]

    disc_tok_rows = [[
        c["token"],
        c["active_listings"],
        f"{c['avg_discount']:.1f}%",
    ] for c in r.top_discount_tokens]

    def _anchor(url, text):
        return f'<a href="{_h.escape(url or "")}">{_h.escape(text or "")}</a>'

    fresh_rows = [[
        e["at"][:19],
        _anchor(e["url"], e["current_title"]),
        e["current_price"],
        e["new_value"],
    ] for e in r.fresh_events]

    repost_rows = [[
        e["at"][:19],
        _anchor(e["url"], e["current_title"]),
        e["current_price"],
        e["current_seller"],
    ] for e in r.repost_events]

    return HTML_TEMPLATE.format(
        window_start=r.window_start,
        window_end=r.window_end,
        days=r.days,
        total_active=r.total_active,
        total_cities=r.total_cities,
        total_categories=r.total_categories,
        total_tokens_tracked=r.total_tokens_tracked,
        top_cities_volume_table=_table(["cidade", "UF", "ativos", "avg price"], vol_rows),
        top_cities_discount_table=_table(["cidade", "UF", "avg disc", "ativos"], disc_city_rows),
        categories_table=_table(["categoria", "ativos", "avg price", "avg disc", "avg liq"], cat_rows),
        top_competition_table=_table(["token", "active", "rem_rate", "vel_d", "comp"], comp_rows),
        top_discount_tokens_table=_table(["token", "n", "avg disc"], disc_tok_rows),
        fresh_events_table=_table(["quando", "anúncio", "preço", "score"], fresh_rows),
        repost_events_table=_table(["quando", "anúncio", "preço", "vendedor"], repost_rows),
        generated_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--out", type=Path)
    args = ap.parse_args()

    report = collect(days=args.days)
    html = render(report)

    REPORTS_DIR.mkdir(exist_ok=True)
    if args.out is None:
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        out = REPORTS_DIR / f"market_report_{day}.html"
    else:
        out = args.out
    out.write_text(html, encoding="utf-8")
    (REPORTS_DIR / "market_latest.html").write_text(html, encoding="utf-8")

    log.info(kv(event="market_report_generated", out=str(out), days=args.days))
    print(f"report written: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
