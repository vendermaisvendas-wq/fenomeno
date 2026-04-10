# BuscaPlace — Facebook Marketplace Intelligence

Sistema de monitoramento e análise de anúncios públicos do Facebook Marketplace.

## Setup

```bash
pip install -r requirements.txt

# Opcionais (ativam funcionalidades extras):
pip install scikit-learn scipy     # price_model backend sklearn
pip install pyarrow                # data_lake + export parquet
pip install polars                 # fast_analytics
```

## Uso rápido

```bash
# 1. Descobrir anúncios por keyword
python discover_links.py hilux diesel

# 2. Extrair dados de um anúncio específico
python extract_item.py 2015275022700246

# 3. Monitorar continuamente (pipeline completo)
python monitor.py --from-db --interval 21600 --concurrency 3

# 4. Criar watcher por keyword + região
python watcher_engine.py create --keyword iphone --region Araçatuba

# 5. Dashboard web
uvicorn web:app --port 8000
```

## Alertas (Telegram / Discord)

Configure via variáveis de ambiente:

```bash
export TELEGRAM_BOT_TOKEN="123:ABC..."
export TELEGRAM_CHAT_ID="-100123456"
export DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/..."
```

## Testes

```bash
pytest tests/
```

## Módulos principais

| Módulo | Função |
|---|---|
| `extract_item.py` | Parser de HTML do Marketplace (4 camadas) |
| `monitor.py` | Pipeline completo de coleta + inteligência |
| `watcher_engine.py` | Monitoramento por keyword + região |
| `market_value.py` | Estimativa de valor de mercado |
| `opportunities.py` | Score de oportunidade (0-100) |
| `alerts.py` | Telegram + Discord webhooks |
| `web.py` | Dashboard FastAPI + Chart.js |
| `marketplace_deep_discovery.py` | Discovery BFS recursivo |
| `continuous_watchers.py` | Daemon para watchers em loop contínuo |

## Dashboard

Após `uvicorn web:app --port 8000`, acesse:

- `/` — listagem de anúncios
- `/watchers` — criar e gerenciar watchers
- `/top-opportunities` — melhores oportunidades por probabilidade
- `/top-deals` — ranking por opportunity_score
- `/stats` — estatísticas gerais + heatmap
- `/geo-insights` — cobertura geográfica
- `/price-trends` — evolução temporal de preços
- `/discovery-network` — grafo de discovery BFS

## Estrutura

```
├── extract_item.py              parser HTML (JSON-LD → OG → Relay → DOM)
├── monitor.py                   pipeline de 20+ etapas
├── watcher_engine.py            watchers keyword+região (sync + async)
├── watcher_scheduler.py         priorização dinâmica de watchers
├── marketplace_deep_discovery.py discovery BFS com discovery_graph
├── market_value.py              ComparablesIndex + estimativa
├── opportunities.py             score + flags heurísticas
├── price_model.py               ML (sklearn) ou fallback kNN
├── liquidity_model.py           score de liquidez
├── fraud_detector.py            fraud_risk_score
├── alerts.py                    Telegram + Discord
├── alert_engine.py              alertas de watcher
├── alert_priority_engine.py     re-ordenação por prioridade
├── web.py                       dashboard FastAPI
├── db.py                        SQLite + migrações idempotentes
├── templates/                   Jinja2 + Bootstrap 5
├── tests/                       352 testes pytest
└── AUDIT_REPORT.md              documentação técnica completa
```

## Licença

Uso pessoal / pesquisa. Respeite os Termos de Serviço do Facebook.
