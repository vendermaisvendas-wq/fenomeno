"""
Gera um relatório semanal HTML com o estado do mercado.

Seções:
    1. Resumo   - counts + deltas vs semana anterior
    2. Top deals da semana  - listings novos com maior opportunity_score
    3. Maiores movimentos de preço (top drops)
    4. Clusters mais ativos (maior número de listings novos)
    5. Tokens com maior desconto médio
    6. Eventos relevantes (parser_break, alertas enviados)

Output: reports/report_YYYY-MM-DD.html + cópia em reports/latest.html.

Uso:
    python weekly_report.py               # última semana
    python weekly_report.py --days 14     # janela customizada
    python weekly_report.py --out custom.html
"""

from __future__ import annotations

import argparse
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from db import connect, init_db
from logging_setup import get_logger, kv
from price_normalizer import parse as parse_price
from title_normalizer import tokens

log = get_logger("weekly_report")

REPORTS_DIR = Path("reports")


@dataclass
class ReportSections:
    window_start: str
    window_end: str
    days: int
    total_active: int
    new_in_window: int
    removed_in_window: int
    price_changes: int
    top_deals: list[dict]
    biggest_drops: list[dict]
    active_clusters: list[dict]
    top_discount_tokens: list[dict]
    events: list[dict]


def _window_bounds(days: int) -> tuple[str, str]:
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)
    return start.isoformat(timespec="seconds"), now.isoformat(timespec="seconds")


def collect(days: int = 7) -> ReportSections:
    init_db()
    start, end = _window_bounds(days)

    with connect() as conn:
        total_active = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE is_removed = 0"
        ).fetchone()[0]

        new_in_window = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE first_seen_at >= ?", (start,),
        ).fetchone()[0]

        removed_in_window = conn.execute(
            "SELECT COUNT(*) FROM listings "
            "WHERE removed_at IS NOT NULL AND removed_at >= ?",
            (start,),
        ).fetchone()[0]

        price_changes = conn.execute(
            "SELECT COUNT(*) FROM events "
            "WHERE event_type = 'price_change' AND at >= ?",
            (start,),
        ).fetchone()[0]

        # Top deals: score mais alto entre os novos
        top_deals = [dict(r) for r in conn.execute(
            """
            SELECT id, current_title, current_price, current_currency,
                   discount_percentage, opportunity_score, url
              FROM listings
             WHERE first_seen_at >= ? AND opportunity_score IS NOT NULL
             ORDER BY opportunity_score DESC, discount_percentage DESC
             LIMIT 15
            """,
            (start,),
        ).fetchall()]

        # Maiores quedas: comparar old/new values dos events de price_change
        drop_rows = conn.execute(
            """
            SELECT e.listing_id, e.old_value, e.new_value, l.current_title, l.url
              FROM events e
              JOIN listings l ON l.id = e.listing_id
             WHERE e.event_type = 'price_change' AND e.at >= ?
            """,
            (start,),
        ).fetchall()

        drops: list[dict] = []
        for r in drop_rows:
            old = parse_price(r["old_value"])
            new = parse_price(r["new_value"])
            if old and new and old > 0:
                pct = (old - new) / old * 100
                if pct > 0:  # só queda
                    drops.append({
                        "id": r["listing_id"],
                        "title": r["current_title"] or "",
                        "url": r["url"],
                        "old_price": old,
                        "new_price": new,
                        "drop_pct": round(pct, 1),
                    })
        drops.sort(key=lambda x: -x["drop_pct"])
        biggest_drops = drops[:10]

        # Clusters mais ativos: novos por cluster_id
        active_clusters = [dict(r) for r in conn.execute(
            """
            SELECT cluster_id, COUNT(*) as n,
                   MIN(current_title) as sample_title
              FROM listings
             WHERE first_seen_at >= ? AND cluster_id IS NOT NULL
             GROUP BY cluster_id
             HAVING n >= 2
             ORDER BY n DESC
             LIMIT 10
            """,
            (start,),
        ).fetchall()]

        # Eventos relevantes
        events = [dict(r) for r in conn.execute(
            """
            SELECT at, listing_id, event_type, new_value
              FROM events
             WHERE at >= ?
               AND event_type IN ('parser_break', 'alert_sent',
                                  'new_opportunity', 'opportunity_flag')
             ORDER BY at DESC
             LIMIT 30
            """,
            (start,),
        ).fetchall()]

        # Tokens com maior desconto médio
        all_active = conn.execute(
            "SELECT current_title, discount_percentage FROM listings "
            "WHERE is_removed = 0 AND current_title IS NOT NULL "
            "AND discount_percentage IS NOT NULL"
        ).fetchall()

    token_buckets: dict[str, list[float]] = {}
    for r in all_active:
        for tok in tokens(r["current_title"]):
            token_buckets.setdefault(tok, []).append(r["discount_percentage"])

    token_discounts = [
        {"token": tok, "avg_discount": round(statistics.fmean(ds), 1), "n": len(ds)}
        for tok, ds in token_buckets.items() if len(ds) >= 5
    ]
    token_discounts.sort(key=lambda x: -x["avg_discount"])
    top_discount_tokens = token_discounts[:15]

    return ReportSections(
        window_start=start,
        window_end=end,
        days=days,
        total_active=total_active,
        new_in_window=new_in_window,
        removed_in_window=removed_in_window,
        price_changes=price_changes,
        top_deals=top_deals,
        biggest_drops=biggest_drops,
        active_clusters=active_clusters,
        top_discount_tokens=top_discount_tokens,
        events=events,
    )


# --- HTML rendering (sem dependência de Jinja2 — report é standalone) ------

HTML_TEMPLATE = """<!doctype html>
<html lang="pt-br">
<head>
<meta charset="utf-8">
<title>Weekly report — {window_end}</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css">
<style>
body {{ padding: 30px; max-width: 1200px; margin: 0 auto; }}
.metric-row .metric {{ padding: 15px; background: #f8f9fa; border-radius: 6px; }}
.metric h2 {{ margin: 0; }}
.metric .label {{ font-size: 0.75rem; text-transform: uppercase; color: #6c757d; letter-spacing: 0.05em; }}
h3 {{ margin-top: 2rem; border-bottom: 2px solid #dee2e6; padding-bottom: 0.5rem; }}
.mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.85rem; }}
.drop {{ color: #198754; font-weight: bold; }}
</style>
</head>
<body>

<h1>Relatório semanal — FB Marketplace Audit</h1>
<p class="text-muted">Janela: {window_start} → {window_end} ({days} dias)</p>

<div class="row g-3 metric-row mb-4">
  <div class="col"><div class="metric"><div class="label">ativos</div><h2>{total_active}</h2></div></div>
  <div class="col"><div class="metric"><div class="label">novos na janela</div><h2>{new_in_window}</h2></div></div>
  <div class="col"><div class="metric"><div class="label">removidos</div><h2>{removed_in_window}</h2></div></div>
  <div class="col"><div class="metric"><div class="label">mudanças de preço</div><h2>{price_changes}</h2></div></div>
</div>

<h3>Top deals novos da semana</h3>
{top_deals_table}

<h3>Maiores quedas de preço</h3>
{drops_table}

<h3>Clusters mais ativos (≥2 novos na janela)</h3>
{clusters_table}

<h3>Tokens com maior desconto médio</h3>
{tokens_table}

<h3>Eventos relevantes (últimos 30)</h3>
{events_table}

<hr>
<p class="text-muted small">Gerado em {generated_at}</p>
</body>
</html>
"""


def _table(headers: list[str], rows: list[list]) -> str:
    if not rows:
        return '<p class="text-muted">(nenhum)</p>'
    import html as _html
    h = "<table class='table table-sm table-striped'><thead><tr>"
    for col in headers:
        h += f"<th>{_html.escape(col)}</th>"
    h += "</tr></thead><tbody>"
    for row in rows:
        h += "<tr>"
        for cell in row:
            h += f"<td>{cell if cell is not None else ''}</td>"
        h += "</tr>"
    h += "</tbody></table>"
    return h


def _a(url: str, text: str) -> str:
    import html as _html
    return f'<a href="{_html.escape(url)}">{_html.escape(text)}</a>'


def render(sections: ReportSections) -> str:
    import html as _html

    top_deals_rows = [[
        r["opportunity_score"],
        _a(r["url"], r["current_title"] or "") if r.get("url") else _html.escape(r.get("current_title") or ""),
        f"{r['current_price']} {r.get('current_currency') or ''}",
        f"{r['discount_percentage']:.0f}%" if r.get("discount_percentage") is not None else "-",
    ] for r in sections.top_deals]

    drops_rows = [[
        _a(r["url"], r["title"]),
        f"{r['old_price']:.0f}",
        f"{r['new_price']:.0f}",
        f"<span class='drop'>-{r['drop_pct']}%</span>",
    ] for r in sections.biggest_drops]

    clusters_rows = [[
        r["cluster_id"],
        r["n"],
        _html.escape((r.get("sample_title") or "")[:80]),
    ] for r in sections.active_clusters]

    tokens_rows = [[
        r["token"],
        r["n"],
        f"{r['avg_discount']}%",
    ] for r in sections.top_discount_tokens]

    events_rows = [[
        r["at"][:19],
        r["event_type"],
        r["listing_id"],
        _html.escape((r.get("new_value") or "")[:80]),
    ] for r in sections.events]

    return HTML_TEMPLATE.format(
        window_start=sections.window_start,
        window_end=sections.window_end,
        days=sections.days,
        total_active=sections.total_active,
        new_in_window=sections.new_in_window,
        removed_in_window=sections.removed_in_window,
        price_changes=sections.price_changes,
        top_deals_table=_table(["score", "título", "preço", "desconto"], top_deals_rows),
        drops_table=_table(["anúncio", "preço antigo", "preço novo", "queda"], drops_rows),
        clusters_table=_table(["cluster_id", "novos", "amostra título"], clusters_rows),
        tokens_table=_table(["token", "n", "desconto médio"], tokens_rows),
        events_table=_table(["quando", "tipo", "listing", "detalhes"], events_rows),
        generated_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--out", type=Path)
    args = ap.parse_args()

    sections = collect(days=args.days)
    html = render(sections)

    REPORTS_DIR.mkdir(exist_ok=True)
    if args.out is None:
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        out = REPORTS_DIR / f"report_{day}.html"
    else:
        out = args.out
    out.write_text(html, encoding="utf-8")
    latest = REPORTS_DIR / "latest.html"
    latest.write_text(html, encoding="utf-8")

    log.info(kv(event="report_generated", out=str(out),
                days=args.days, top_deals=len(sections.top_deals)))
    print(f"report written: {out}")
    print(f"symlinked:      {latest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
