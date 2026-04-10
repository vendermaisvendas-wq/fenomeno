# BuscaPlace — Monitoramento do Facebook Marketplace

Sistema de monitoramento e análise de anúncios públicos do Facebook Marketplace.

## Instalação

```bash
pip install -r requirements.txt
```

## Como usar

### 1. Diagnóstico do sistema
```bash
python system_check.py
```

### 2. Inserir dados de teste (sem internet)
```bash
python seed_test_data.py
```

### 3. Descobrir anúncios reais
```bash
python discover_links.py iphone
python discover_links.py hilux diesel
```

### 4. Criar monitoramento por palavra-chave
```bash
python watcher_engine.py create --keyword iphone --region Araçatuba
python watcher_engine.py backfill 1
python watcher_engine.py list
```

### 5. Rodar pipeline completo
```bash
python monitor.py --from-db --once
```

### 6. Abrir dashboard
```bash
uvicorn web:app --port 8000
```
Acesse: **http://localhost:8000**

## Principais páginas do dashboard

| Página | O que mostra |
|---|---|
| `/` | Lista de anúncios |
| `/watchers` | Criar e gerenciar monitoramentos |
| `/top-opportunities` | Melhores oportunidades |
| `/stats` | Estatísticas gerais |
| `/geo-insights` | Cobertura geográfica |
| `/system-status` | Status do sistema |

## Alertas (Telegram / Discord)

Configure via variáveis de ambiente:

```bash
export TELEGRAM_BOT_TOKEN="seu_token"
export TELEGRAM_CHAT_ID="seu_chat_id"
export DISCORD_WEBHOOK_URL="sua_url_webhook"
```

## Testes

```bash
pytest tests/
```

## Diagnóstico rápido

```bash
python system_check.py                    # verifica tudo
python system_check.py --skip-network     # sem internet
```

## Licença

Uso pessoal / pesquisa. Respeite os Termos de Serviço do Facebook.
