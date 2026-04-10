# Auditoria técnica — FB Marketplace (superfície pública)

**Escopo:** o que é obtenível de páginas públicas (`/marketplace/item/{id}/`) sem autenticação, via requisição HTTP ou navegador. Fora de escopo: bypass de login wall, enumeração de IDs, reprodução de GraphQL autenticado, evasão de rate limit.

**Aviso de estabilidade:** o Facebook minifica/ofusca HTML e muda nomes de campos. Seletores estruturais (OpenGraph, `<title>`) são estáveis; chaves Relay (`marketplace_listing_title`, etc.) são observáveis mas mudam periodicamente. Teste o parser contra o HTML atual antes de depender dele.

---

## 1. Arquitetura do projeto

```
FB_Search/
├── db.py                       camada única SQLite (schema, helpers, vacuum)
├── logging_setup.py            configuração central de logging
├── html_cache.py               cache de HTML bruto p/ debug e reprocessamento
├── extract_item.py             parser em 4 camadas, sync/async, --debug-html
├── extract_item_playwright.py  fallback com navegador real
├── discover_links.py           descoberta de URLs via buscador (DDG plugável)
├── monitor.py                  loop assíncrono, reconciliação, price_history
├── opportunities.py            heurísticas p/ flagar oportunidades
├── parser_health.py            healthcheck das camadas do parser
├── analytics.py                estatística por keyword
├── web.py                      dashboard FastAPI + Chart.js
├── templates/                  Jinja2 + Bootstrap 5
│   ├── base.html
│   ├── index.html              listagem
│   ├── item.html               detalhe + gráfico de preço
│   ├── explorer.html           busca por substring
│   ├── stats.html              métricas + distribuição de preços
│   └── opportunities.html      listagem filtrada por flag
├── tests/                      pytest (31 testes)
│   ├── conftest.py             fixtures HTML sintéticas
│   ├── test_parser.py          1 teste por camada + edge cases
│   ├── test_opportunities.py   heurísticas puras
│   ├── test_db.py              schema + inserts + vacuum
│   └── test_analytics.py       _to_float + compute_stats
├── logs/                       (criado em runtime) monitor.log rotativo
├── html_cache/                 (criado em runtime) HTMLs para debug
├── marketplace.sqlite3         (criado em runtime) banco WAL
├── requirements.txt
└── AUDIT_REPORT.md             (este documento)
```

Fluxo de dados:

```
discover_links.py  ──┐
                     │ (insere id+url com status='pending')
seed_urls.txt   ─────┤
                     ▼
            ┌────────────────┐
            │    monitor.py  │  (async, httpx, sem(3), jitter 3–10s)
            │                │
            │  extract_async │──▶ parse_html (L1..L4) ──▶ Listing
            │       │        │
            │       ▼        │
            │   reconcile    │  (diff vs. DB, gera eventos)
            └────────┬───────┘
                     │
                     ▼
            ┌────────────────┐
            │  db.py (SQLite) │
            │  listings       │
            │  snapshots      │
            │  events         │
            └────────┬───────┘
                     │
      ┌──────────────┴──────────────┐
      ▼                             ▼
 analytics.py                    web.py
 (stats por keyword)            (dashboard)
```

---

## 2. Modelo de renderização do Marketplace

- **React + Relay Modern + SSR híbrido.** O HTML inicial vem com dados pré-carregados embutidos que hidratariam a store do Relay no cliente.
- **ScheduledServerJS**: mecanismo do FB que executa payloads JSON em ordem via `require(["ScheduledServerJS"]).handle({...})` espalhados pelo HTML.
- Sem cookies, um `GET /marketplace/item/{id}/` responde `200` com HTML completo. Um modal de login é inserido via JS, mas o HTML base continua legível.
- Queries subsequentes pós-hidratação (chat, "mais deste vendedor") atingem `/api/graphql/` com cookies válidos — não estão na superfície pública e **não tentamos reproduzi-las**.

**Implicação:** para o estado inicial de um item, `httpx` + parser de HTML basta. Playwright só entra como fallback.

---

## 3. Camadas de parsing

O extrator em [extract_item.py](extract_item.py) aplica 4 camadas em ordem. Cada campo extraído registra a camada em `field_sources: dict[str, str]`.

### L1 — JSON-LD
Procura `<script type="application/ld+json">` e extrai `@type=Product/Offer/IndividualProduct`. O Marketplace raramente expõe JSON-LD hoje, mas a camada existe como contrato estável do schema.org para o dia que voltar.

### L2 — OpenGraph + meta tags
Mais estável na prática. Campos esperados:
```
og:title, og:description, og:image, og:url
product:price:amount, product:price:currency
```
O FB precisa dessas tags para previews em WhatsApp/Twitter, então elas são improváveis de sumir. Limitação: `og:description` vem truncada, `og:image` é só a foto principal.

### L3 — Relay regex sobre HTML bruto
Chaves internas observáveis via regex direta:
```
marketplace_listing_title        → título completo
listing_price.amount / currency  → preço
listing_price.formatted_amount   → preço já formatado ("R$ 1.234,00")
redacted_description.text        → descrição completa
location_text.text               → "São Paulo, SP"
marketplace_listing_category_name
marketplace_listing_seller.name  → frequentemente ausente sem login
creation_time                    → epoch seconds
uri/image com scontent*.fbcdn.net → todas as fotos
```
Camada mais frágil (nomes podem mudar), mas mais completa que L2.

### L3.5 — walk recursivo em `<script type="application/json">`
Backup estrutural: parseia os chunks JSON embutidos e vasculha por dicts com chaves conhecidas. Tolera mudanças no chunking.

### L4 — DOM fallback com BeautifulSoup
As classes CSS do FB são ofuscadas (`x193iq5w xeuugli x13faqbe`), então só podemos confiar em estrutura grosseira: primeiro `<h1>` para título, regex `R\$\s*[\d.,]+` sobre o texto visível para pegar ao menos um preço formatado. Último recurso.

**Provenance tracking:** cada camada chama `_mark(listing, attr, value, "og"|"relay"|…)` que só grava se o campo ainda estiver vazio. Assim as camadas mais estáveis ganham prioridade e sabemos quem preencheu cada campo — útil para debug quando o FB muda o HTML.

---

## 4. Superfície pública por rota (observado)

| Rota | Sem login | Notas |
|---|---|---|
| `/marketplace/item/{id}/` | **Acessível** | Modal de login, mas HTML tem dados |
| `/marketplace/` | Parcial | Layout, some rápido |
| `/marketplace/{cidade}/search/?query=` | Instável | Alguns resultados antes do wall |
| `/marketplace/category/{slug}` | Instável | Idem |
| `/marketplace/profile/{seller_id}` | Wall | Login obrigatório |

Campos que **somem** sem login em item público: nome/avatar do vendedor (às vezes), "More from this seller", contador de views. Campos que **permanecem**: título, preço, moeda, descrição completa, cidade, categoria, todas as fotos, data de criação, atributos da categoria.

**Detecção de login wall / not-found:** [extract_item.py:75-94](extract_item.py#L75-L94)
- Login: `id="login_form"` ou redirect para `/login`
- Not found: "this content isn't available" / "este conteúdo não está disponível"

Ambos viram `status = "login_wall"` / `"not_found"` no Listing e não continuam para parsing.

---

## 5. Schema do banco

Definido em [db.py:11](db.py#L11). Três tabelas:

### `listings`
Estado corrente de cada anúncio. Campos relevantes:
- `source` — `manual | seed | monitor | discover:ddg` — de onde veio o ID
- `last_status` — `pending | ok | login_wall | not_found | error | empty`
- `is_removed`, `removed_at`, `reappeared_at` — ciclo de vida
- `current_title`, `current_price`, `current_currency`, `current_location` — último estado conhecido

### `snapshots`
Histórico append-only. Cada fetch cria um snapshot com `payload_hash` (SHA-256 de título+preço+descrição+status, 16 chars) + `payload_json` completo. O hash serve para detectar "no-change" rapidamente no futuro.

### `events`
Eventos discretos. `event_type ∈ {first_seen, price_change, title_change, status_change, removed, reappeared}` com `old_value`/`new_value`.

A lógica de reconciliação vive em [monitor.py:94](monitor.py#L94) (`reconcile()`). Ela:
1. Lê a linha atual do listing
2. Compara campo a campo com o novo Listing
3. Emite eventos apropriados
4. Atualiza flags de `is_removed`/`reappeared`
5. Faz UPDATE consolidado na tabela `listings`
6. Insere snapshot

---

## 6. Coleta assíncrona

Implementada em [monitor.py:163](monitor.py#L163) com `httpx.AsyncClient` + `asyncio.Semaphore(concurrency)`. Características:

- **Concorrência limitada**: default 3, configurável via `--concurrency`. Nunca recomendo passar disso — ganho marginal, risco alto.
- **Jitter por request**: `random.uniform(min_delay, max_delay)` entre 3s e 10s. Sem isso o padrão fica previsível.
- **Jitter entre ciclos**: ±5% do intervalo para quebrar cadência.
- **Pausa global em login wall**: a variável `STATE.login_wall_pause_until` armazena um timestamp do event loop; toda task que chega em `fetch_one` respeita essa pausa antes de fazer IO. Se um fetch detecta wall, ela é estendida por 10min. Isso impede que 3 workers continuem batendo em paralelo depois de um bloqueio.
- **Shutdown limpo**: SIGINT seta `STATE.stop`, tasks em voo terminam e o loop sai.

Observação: usar `get_running_loop()` (não `get_event_loop()` — deprecated em 3.12+).

---

## 7. Descoberta de links

[discover_links.py](discover_links.py) usa um backend de busca pluggable com `DuckDuckGoBackend` padrão. Por que DDG e não Google:

- DDG HTML (`html.duckduckgo.com/html/`) é mais tolerante a tráfego automatizado, sem chave
- Google HTML scraping cai em reCAPTCHA em poucos requests
- Para cobertura real e escala, o caminho honesto é **API paga de SERP** (SerpAPI/Serper/Oxylabs) — basta implementar outro `SearchBackend` e plugar

Query construída: `site:facebook.com/marketplace/item <keywords>`. O parser pega `a.result__a`, desembrulha o redirect `/l/?uddg=...` de DDG, filtra por `facebook.com/marketplace/item/(\d+)`, canoniza a URL e insere no banco via `db.discover_insert()` com `status='pending'`.

Limitações honestas:
- Cobertura de indexação é parcial — o buscador só conhece o que foi indexado algum dia
- DDG retorna ~30 resultados por página; rodar mais de 2–3 páginas aumenta risco de CAPTCHA
- Anúncios "quentes" muitas vezes ainda não foram indexados

---

## 8. Analytics

[analytics.py](analytics.py) lê listings ativos do banco e agrupa por substring no título (case-insensitive).

- Preços são parseados de string via `_to_float()` que lida com formato BR ("R$ 1.234,56") e internacional ("1234.56")
- Por grupo: `count, mean, median, stdev (populacional), min, max, currency`
- Usa `statistics` da stdlib — zero dependências extras
- Modo `--all` ignora keywords e dá estatística global
- Modo `--json` para pipe em outras ferramentas

Exemplo:
```
$ python analytics.py hilux iphone bicicleta
keyword                  n         mean       median        stdev        min        max cur
hilux                   12   145,833.00   142,000.00    28,441.00  98,000.00  210,000.00 BRL
iphone                   8     3,412.00     3,500.00       612.00   1,800.00    4,500.00 BRL
bicicleta                3       850.00       800.00       175.00     650.00    1,100.00 BRL
```

---

## 9. Dashboard web

[web.py](web.py) é um app FastAPI **read-only** sobre o mesmo SQLite. Não faz fetch. Rotas:

- `GET /` — últimos 500 listings, com contadores de total/removidos
- `GET /item/{id}` — detalhes: metadados, `field_sources` do último snapshot, fotos, eventos, lista de snapshots
- `GET /explorer?q=...` — busca por substring no título
- `GET /api/stats` — contadores JSON (healthcheck)

Templates Jinja2 em [templates/](templates/) usam Bootstrap 5 via CDN. Classes CSS mínimas. Jinja2 auto-escapa valores — a descrição do anúncio é exibida com `white-space: pre-wrap` mas continua escapada, sem risco de XSS.

Para rodar: `uvicorn web:app --reload --port 8000`.

---

## 10. Rate limit e comportamento polido

- User-Agent identificando a ferramenta + e-mail de contato (ver [extract_item.py:36](extract_item.py#L36))
- Concorrência máx 3
- Delay 3–10s aleatório por request
- Jitter entre ciclos
- Pausa global de 10min se qualquer request voltar `login_wall`
- Parar imediatamente e investigar se wall virar permanente
- Nunca re-fetchar o mesmo ID mais de uma vez em poucos minutos
- Cache agressivo via `payload_hash` — snapshots idênticos não geram eventos

---

## 11. Como executar cada parte

Setup:
```bash
pip install -r requirements.txt
# opcional, só se for usar extract_item_playwright.py
playwright install chromium
```

Inicializar o banco (primeira vez ou qualquer vez — é idempotente):
```bash
python -c "import db; db.init_db()"
```

Extrair um item único (debug rápido):
```bash
python extract_item.py 2015275022700246
# ou
python extract_item.py https://www.facebook.com/marketplace/item/2015275022700246/
```

Descobrir URLs por palavra-chave (insere no banco):
```bash
python discover_links.py moto
python discover_links.py iphone 13
python discover_links.py --max-pages 3 --dry-run carro
```

Monitorar (uma passada) um arquivo seed:
```bash
echo "https://www.facebook.com/marketplace/item/2015275022700246/" > seed_urls.txt
python monitor.py --seed seed_urls.txt --once
```

Monitorar continuamente tudo que está no DB (incluindo descobertos):
```bash
python monitor.py --from-db --interval 21600 --concurrency 3
```

Analytics:
```bash
python analytics.py hilux cg iphone
python analytics.py --all
python analytics.py --json iphone > iphone_stats.json
```

Dashboard:
```bash
uvicorn web:app --reload --port 8000
# abra http://localhost:8000
```

Pipeline típico de uso:
```bash
python discover_links.py hilux diesel
python discover_links.py hilux 4x4
python monitor.py --from-db --once     # primeiro fetch dos pendentes
python analytics.py hilux
uvicorn web:app --port 8000
```

---

## 12. Observabilidade e resiliência (v3)

### 12.1 Logging estruturado

Configuração central em [logging_setup.py](logging_setup.py). Logger raiz `fb_search.*`, saída em `logs/monitor.log` com rotação (5MB × 5 arquivos). Console recebe apenas `WARNING+`. Helper `kv(**kwargs)` produz pares `key=value` consistentes.

Formato:
```
2026-04-10T14:30:00 [INFO] fb_search.monitor: listing=2015275022700246 event=price_change
2026-04-10T14:31:05 [INFO] fb_search.extract: listing=2015275022700246 status=ok method=og+relay price="R$ 13.500" title_len=42
2026-04-10T14:45:12 [WARNING] fb_search.monitor: event=login_wall_detected pause_for=600.0
```

Chamadas relevantes:
- [extract_item.py:parse_html](extract_item.py) — loga `status`, `method`, `price`, `title_len` em cada extração bem-sucedida; `WARNING` em login wall / empty; `ERROR` em falhas HTTP
- [monitor.py:run_pass](monitor.py) — loga início/fim de passada, cada evento reconciliado, pausa por login wall, resumo do scan de opportunities
- [opportunities.py:scan](opportunities.py) — loga cada flag emitida com rule + listing_id
- [parser_health.py](parser_health.py) — loga o veredito final do healthcheck

### 12.2 Parser health check

[parser_health.py](parser_health.py) pega uma amostra aleatória de N listings ativos (default 10), roda `extract()` em cada um, e tabula quais camadas preencheram quais campos. Saída em texto ou JSON.

Exemplo:
```
sample_size: 10
statuses:    {'ok': 9, 'login_wall': 1}

layer coverage (% of OK listings that used this layer):
  jsonld        0.0%
  og          100.0%  ████████████████████
  relay        88.9%  █████████████████
  json_walk    66.7%  █████████████
  dom           0.0%

field coverage (% of OK listings that have this field, + source layers):
  title                 100.0%   [og=9]
  price_amount          100.0%   [og=7, relay=2]
  price_formatted        88.9%   [relay=8]
  description            88.9%   [og=9]
  location_text          77.8%   [relay=7]
  creation_time          66.7%   [relay=6]
  category               55.6%   [relay=5]

verdict: HEALTHY
```

**Como detectar regressão:** monitore o veredito. `degraded` quando OK rate < 85% ou camada OG < 50%. `broken` quando OK rate < 50%. O campo mais valioso é a tabela por-campo: quando "price_amount" cai de 100% OG para 0% OG (mas continua 100% via Relay), você sabe que o FB tirou a meta tag de preço mas o Relay store ainda tem. Vice-versa identifica mudança nas chaves internas.

Modo offline: `parser_health.py --from-cache` usa HTMLs em `html_cache/` ao invés de fazer fetch. Útil para testar alterações no parser sem tocar no FB.

### 12.3 Cache de HTML

[html_cache.py](html_cache.py) — layout `html_cache/{id}.html` e `{id}.json` (Listing extraído). Usado por:
- `extract_item.py --debug-html`: salva HTML bruto + JSON da extração, imprime tabela de `field_sources` por campo
- `parser_health.py --from-cache`: reparseia HTMLs cached sem fetch
- `monitor.py`: pode ser ativado passando `cache=True` para `extract_async` (não exposto no CLI do monitor ainda para evitar crescimento descontrolado do diretório)

API:
```python
from html_cache import save_html, load_html, save_listing_json
save_html("123", html_text)
html = load_html("123")
```

### 12.4 Histórico completo de preço

Nova tabela [db.py:price_history](db.py):
```sql
CREATE TABLE price_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id  TEXT NOT NULL,
    price       REAL NOT NULL,
    price_raw   TEXT,        -- string original pré-parsing
    currency    TEXT,
    recorded_at TEXT NOT NULL
);
```

Gravada por [monitor.py:_record_price_history](monitor.py) em dois momentos:
1. No `first_seen` quando status=ok (baseline)
2. Em cada `price_change` detectado em `reconcile()`

Parsing do preço (string → float) é delegado a `analytics._to_float`, que agora suporta robustamente formato BR com e sem vírgula decimal (`"R$ 185.000"` → `185000`, `"R$ 1.234,56"` → `1234.56`). Bug encontrado pelos testes — corrigido.

### 12.5 Detecção de oportunidades

[opportunities.py](opportunities.py) implementa 4 regras, todas puras (testáveis sem DB):

| Rule | Critério |
|---|---|
| `urgency_keyword` | título contém "urgente", "preciso vender", "hoje", "desapego", "queima", etc. |
| `short_description` | descrição ausente ou < 30 caracteres |
| `price_drop` | histórico de preço mostra queda ≥ 15% entre primeiro e último ponto |
| `below_market` | preço < média − 1.5σ do grupo de listings com token comum no título (n ≥ 4) |

Flags são gravadas como `event_type='opportunity_flag'` com `new_value="<rule>: <reason>"`. Deduplicação por `_already_flagged()` — a mesma regra não é refletida duas vezes para o mesmo listing.

`scan()` é chamado automaticamente ao fim de cada `run_pass()` do monitor. Também pode rodar manualmente:
```bash
python opportunities.py           # scan + imprime sumário
python opportunities.py --dry-run # não grava
```

### 12.6 Otimização de banco

Novos índices em [db.py:SCHEMA](db.py):
- `idx_listings_last_seen`, `idx_listings_status`, `idx_listings_removed`
- `idx_snap_fetched_at` (complementa `idx_snap_listing`)
- `idx_events_type` (acelera `/opportunities` e o GROUP BY em `/stats`)
- `idx_ph_listing`, `idx_ph_price`, `idx_ph_recorded`

PRAGMA `journal_mode = WAL` ativado — permite leituras do dashboard sem bloquear escritas do monitor.

`vacuum_database()` em [db.py](db.py) roda `VACUUM` + `ANALYZE`, retorna métricas de bytes recuperados. Chame periodicamente (semana/mês).

### 12.7 Dashboard expandido

Novas rotas em [web.py](web.py):
- `GET /stats` — cards com contadores (total/ativos/removidos/snapshots/price_history), tabelas de distribuição por status/source/event_type, histograma de preços via Chart.js (`/api/price_distribution`)
- `GET /opportunities[?rule=...]` — lista de listings flagados, filtrável por regra, com contagem por rule em badges
- `GET /api/price_history/{id}` — JSON para Chart.js
- `GET /api/price_distribution?bins=20` — histograma

Página de item ([templates/item.html](templates/item.html)) agora inclui gráfico de linha do histórico de preço quando há ≥ 1 ponto em `price_history`.

### 12.8 Testes automáticos

31 testes em [tests/](tests/), todos passando. Executar com `pytest tests/`.

- [tests/test_parser.py](tests/test_parser.py) — 8 testes: JSON-LD, OG (com strip de suffix), Relay (chaves internas), DOM fallback, login wall, not found, empty, prioridade entre camadas. Fixtures HTML sintéticas em [tests/conftest.py](tests/conftest.py) — cada fixture dispara apenas uma camada, garantindo isolamento.
- [tests/test_opportunities.py](tests/test_opportunities.py) — 11 testes: cada regra heurística com match e no-match, edge cases (n insuficiente, histórico curto).
- [tests/test_db.py](tests/test_db.py) — 6 testes: schema, idempotência de discover_insert, snapshots, events, price_history ordenado, vacuum. Usa SQLite em tmp_path, sem tocar o banco real.
- [tests/test_analytics.py](tests/test_analytics.py) — 6 testes: parser de preço BR/internacional, stats por keyword, case-insensitive.

**Bug real caçado pelos testes:** `_to_float("R$ 185.000")` retornava `185.0` (dot interpretado como decimal). Heurística corrigida para detectar ponto como separador de milhares quando o último grupo tem 3 dígitos.

### 12.9 Modo `--debug-html` em extract_item

```bash
python extract_item.py 2015275022700246 --debug-html
```

Faz:
1. Fetch normal do item
2. Salva HTML bruto em `html_cache/2015275022700246.html`
3. Salva JSON da extração em `html_cache/2015275022700246.json`
4. Imprime tabela de `field_sources` (qual camada preencheu cada campo) + valores resumidos

Saída:
```
============================================================
  id:      2015275022700246
  status:  ok
  method:  og+relay
------------------------------------------------------------
  field sources (which layer filled each field):
    description            <- og
    primary_image_url      <- og
    price_amount           <- og
    price_currency         <- og
    title                  <- og
    location_text          <- relay
    category               <- relay
    creation_time          <- relay
    price_formatted        <- relay
    title                  = Moto Honda CG 160 Titan 2021
    price_amount           = 13500
    price_currency         = BRL
    ...
============================================================

  saved: html_cache/2015275022700246.html
  saved: html_cache/2015275022700246.json
```

---

## 13. Como executar cada recurso novo

```bash
# Health check do parser
python parser_health.py                    # amostra 10 do DB
python parser_health.py --sample 20
python parser_health.py --from-cache       # offline, a partir de html_cache/
python parser_health.py --json             # para pipe / monitoring

# Debug de parser numa URL específica
python extract_item.py 2015275022700246 --debug-html

# Scan manual de oportunidades
python opportunities.py
python opportunities.py --dry-run

# Vacuum periódico
python -c "import db; print(db.vacuum_database())"

# Dashboard expandido
uvicorn web:app --reload --port 8000
#   http://localhost:8000/           listings
#   http://localhost:8000/stats      métricas + histograma
#   http://localhost:8000/opportunities
#   http://localhost:8000/item/{id}  gráfico de preço
#   http://localhost:8000/api/price_history/{id}

# Tests
pytest tests/
pytest tests/ -v
pytest tests/test_parser.py -v
```

Pipeline típico de uso em produção:
```bash
# dia 0: descobrir seed inicial
python discover_links.py hilux diesel
python discover_links.py iphone 13

# dia 0..∞: monitor em loop
python monitor.py --from-db --interval 21600 --concurrency 3
#   → loga em logs/monitor.log
#   → atualiza price_history a cada mudança
#   → scan de opportunities ao fim de cada passada

# semana 1: checar saúde do parser
python parser_health.py

# semana 1: analytics
python analytics.py hilux iphone

# dashboard sempre disponível em paralelo
uvicorn web:app --port 8000

# mês 1: vacuum
python -c "import db; print(db.vacuum_database())"
```

---

## 14. Inteligência de mercado (v4)

A camada de inteligência transforma o monitor em um radar de oportunidades. Cada módulo é pequeno, puro onde possível, e integra via tabelas existentes (SQLite) + eventos.

### 14.1 Novas colunas em `listings`

Migração aplicada em [db.py:_migrate_columns](db.py). Colunas novas (todas `NULL`-ok):

| Coluna | Tipo | Preenchida por |
|---|---|---|
| `estimated_market_value` | REAL | `market_value.recompute_all()` |
| `discount_percentage`    | REAL | `market_value.recompute_all()` |
| `opportunity_score`      | INTEGER | `opportunities.score_all_listings()` |
| `cluster_id`             | INTEGER | `duplicate_detector.cluster_all()` |

**Migração idempotente:** `init_db()` primeiro executa o schema base, depois chama `_migrate_columns()` que faz `ALTER TABLE ADD COLUMN` só para as colunas que ainda não existem (via `PRAGMA table_info`). Depois cria índices v4 que referenciam essas colunas. Bancos legados da v3 evoluem automaticamente sem perda de dados — testado em `test_db.py::test_migration_adds_missing_columns`.

### 14.2 `title_normalizer.py` (fundacional, puro)

Tokenização e normalização de títulos para comparar anúncios parecidos.

- `normalize(text)` — lowercase, strip accents, sem pontuação redundante
- `tokens(text)` — set de tokens ≥2 chars, sem stopwords PT-BR
- `extract_year(text)` — primeiro ano 1980–2049 (regex restrito para não casar "R$ 2020")
- `extract_brand(text)` — marcas conhecidas (veículos + eletrônicos), iteração determinística (ordenada) para dar o mesmo resultado em runs diferentes
- `signature(title)` — `(brand, frozenset(model_tokens_sem_ano_sem_marca), year)` — chave de agrupamento
- `jaccard(a, b)` — similaridade `|a∩b|/|a∪b|`; vazio∩vazio convenciona 1.0

Stopwords incluem vocabulário de marketplace: "vendo", "aceito", "troco", "urgente", "desapego", etc. Marcas listadas como constantes no topo — fácil de estender.

### 14.3 `market_value.py`

**Objetivo:** para cada listing com preço, estima valor de mercado via mediana de comparáveis.

**Cascata de comparáveis** em [find_comparables](market_value.py):
1. Mesma marca + mesmo ano (N ≥ 3) — estimativa mais apertada
2. Mesma marca, qualquer ano (N ≥ 3) — relaxa o ano
3. Jaccard ≥ 0.5 sobre tokens — fallback para itens sem marca reconhecida (bicicletas, móveis, etc.)

Para cada item com ≥ 3 comparáveis:
```
estimated_market_value = mediana(prices_comparáveis)
discount_percentage = (mediana − preço) / mediana × 100
```

Valores positivos de `discount_percentage` = preço abaixo do mercado (oportunidade). Negativos = preço acima.

Também expõe `token_group_stats(min_count=5)` que retorna `{token → GroupStats(count, mean, median, p25, p75, stdev)}` para todos os tokens que aparecem em ≥ N listings. Usado pelo `/api/price_heatmap`.

**Percentil** implementado na mão ([percentile](market_value.py)) — método linear tipo R's type 7, sem dependência numpy. Testado com conjuntos pares e ímpares.

CLI:
```bash
python market_value.py            # recomputa tudo
python market_value.py --dry-run  # só imprime sumário
```

### 14.4 Opportunity score (0–100)

Adicionado em [opportunities.py:compute_score](opportunities.py) como função pura. Pesos:

| Sinal | Peso | Fonte |
|---|---|---|
| `discount > 30%` | 40 | `listings.discount_percentage` |
| `discount > 15%` (mid) | 20 | idem |
| `below_p25_proxy` (discount > 25%) | 15 | proxy em cima do discount |
| `urgency_keyword` no título | 15 | lista em `URGENT_PATTERNS` |
| `short_description` (< 30 chars) | 10 | último snapshot payload |
| `recent < 2h` (first_seen) | 20 | `listings.first_seen_at` |

Score é clippado em 100. Retorna `(score, reasons_list)` para debugging.

`score_all_listings()` recomputa para todos os ativos e faz UPDATE em batch. Rodado automaticamente pelo monitor ao fim de cada passada.

Score ≥ 80 → dispara alerta. Ordenação do `/top-deals` é por score desc.

### 14.5 `new_listing_detector.py`

Foca nos listings com `first_seen < 2h`. Flagra como `event_type='new_opportunity'` se:

- **Forte:** `discount_percentage ≥ 20%` OU (popular_keyword + discount > 10%)
- **Fraco:** ≥ 2 sinais entre {popular_keyword, discount > 5%, age < 2h}

POPULAR_KEYWORDS é lista hard-coded de itens com demanda alta e liquidez rápida (iPhone, Hilux, PlayStation, etc.). Extensível.

Dedup: se já existe evento `new_opportunity` para aquele listing, não flagra de novo.

Ordem de execução importa: **deve rodar depois de `market_value.recompute_all()`** porque depende de `discount_percentage` estar atualizado. O monitor já chama na ordem certa.

### 14.6 `alerts.py` (Telegram + Discord)

Webhooks via `httpx`. Configuração por env vars — canais não configurados são silenciosamente pulados.

```bash
export TELEGRAM_BOT_TOKEN=123:ABC
export TELEGRAM_CHAT_ID=-100123456
export DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/.../...
```

**Quando alertar** (função [should_alert](alerts.py)):
- `opportunity_score ≥ 80` OU
- `discount_percentage ≥ 30`

**Formato da mensagem:**
```
🔔 Oportunidade score=85
iPhone 13 128GB preto, bateria 92%
preço: 2800 BRL
desconto: 35%
valor estimado: 4300
https://www.facebook.com/marketplace/item/12345/
```

**Idempotência por canal:** antes de enviar, `_already_alerted(listing, channel)` checa se existe evento `event_type='alert_sent'` com `old_value='<channel>'` para aquele listing. Um mesmo deal é notificado uma vez por canal — mesmo que o scanner rode 10 vezes.

CLI:
```bash
python alerts.py              # scan + envio
python alerts.py --dry-run    # lista sem enviar
python alerts.py --test       # mensagem de teste nos canais configurados
```

### 14.7 `duplicate_detector.py`

Agrupa anúncios possivelmente duplicados em clusters. Critério de duplicata (todos precisam bater):

1. `jaccard(tokens(a), tokens(b)) ≥ 0.7`
2. `|preçoA − preçoB| / min ≤ 10%` (se ambos tiverem preço)
3. Mesma cidade (se ambos tiverem localização)

Algoritmo: [UnionFind](duplicate_detector.py) O(n²) sobre listings ativos. Aceitável até alguns milhares. Para escala maior seria preciso "blocking" (comparar só dentro de cada marca/cidade primeiro) — trade-off aceito.

Resultado: `cluster_id` preenchido em todos os listings; clusters de tamanho 1 são singletons (sem dup). Sumário retorna quantos clusters multi-membro existem.

CLI: `python duplicate_detector.py`.

### 14.8 Pipeline integrado no monitor

Em [monitor.py:_run_intelligence_pipeline](monitor.py), ao fim de cada passada (`run_pass`), roda em ordem:

```
1. market_value.recompute_all()           → atualiza estimated_market_value, discount_percentage
2. opportunities.scan()                    → flags heurísticas legadas (urgency, price_drop, etc.)
3. opportunities.score_all_listings()      → recalcula opportunity_score
4. new_listing_detector.scan()             → novas oportunidades
5. alerts.scan_and_alert()                 → envia alertas
6. duplicate_detector.cluster_all()        → atualiza cluster_id
```

Cada etapa roda em `try/except` isolado. Falha numa etapa loga ERROR mas não derruba as outras — o monitor sempre tenta progredir.

Log no console (formato enxuto) + `logs/monitor.log` estruturado (formato kv). Exemplo do console:
```
  [pipeline] market=43  opps=7  scored=58  new=2  alerts_tg=1  alerts_dc=1  clusters=4
```

### 14.9 Rotas novas no dashboard

- **`GET /top-deals?limit=50`** — lista ordenada por `opportunity_score DESC, discount_percentage DESC`. Template [templates/top_deals.html](templates/top_deals.html) colore score (vermelho ≥80, amarelo ≥50) e desconto (vermelho >30%, amarelo >15%). Mostra preço atual vs. `estimated_market_value` lado a lado, e o `cluster_id` para identificar duplicatas.
- **`GET /api/price_heatmap?min_count=5`** — retorna top 40 tokens por contagem, com `count, mean, median, p25, p75, stdev`. Usado pelo chart no `/stats`.

O `/stats` agora renderiza 2 gráficos:
1. **Heatmap por keyword** — barras = mediana de preço por token, linha sobreposta = contagem (eixo direito). Identifica imediatamente "quais nichos têm mais anúncios e em que faixa de preço".
2. **Distribuição global** (já existia) — histograma em bins.

Nav atualizada em [templates/base.html](templates/base.html) incluindo "Top Deals".

### 14.10 Testes adicionados

60 novos testes, total agora 91 passando em ~0.6s:

| Arquivo | Testes | Cobertura |
|---|---|---|
| `test_title_normalizer.py` | 14 | normalize, tokens, stopwords, extract_year range, extract_brand, signature, jaccard |
| `test_market_value.py` | 11 | percentile (ímpar, par, singleton, vazio), compute_group_stats, find_comparables (year-match, year-relax, jaccard-fallback, self-exclusion) |
| `test_duplicate_detector.py` | 10 | is_similar (positivos e negativos por critério), UnionFind, _city_of |
| `test_score.py` | 9 | baseline zero, cada sinal isoladamente, soma máxima, clipping em 100 |
| `test_alerts.py` | 7 | should_alert, format_message (com e sem opcionais), env var missing |
| `test_new_detector.py` | 7 | is_recent (naive vs aware), has_popular_keyword |
| `test_db.py` (ampliado) | 8 | + `test_schema_has_intelligence_columns`, + `test_migration_adds_missing_columns` que constrói um banco legado v3 à mão e verifica que `init_db()` aplica ALTER TABLE |

### 14.11 Bugs reais encontrados pelos testes (v4)

1. **Ordering do schema vs migração** — Meu primeiro init_db executava `SCHEMA` completo e depois chamava `_migrate_columns()`. Mas o schema continha `CREATE INDEX idx_listings_score ON listings(opportunity_score)`, referenciando uma coluna que só seria adicionada pela migração. Bancos legados v3 davam erro `no such column: opportunity_score`. Caçado por `test_migration_adds_missing_columns`. **Correção:** separei `SCHEMA_V4_INDICES` que roda após `_migrate_columns()`.

2. **Expectativa de teste errada em `find_comparables`** — Pool com só 2 itens mesmo ano (abaixo do `MIN_COMPARABLES=3`) deveria relaxar para mesma marca. Meu teste esperava que não relaxasse. O relaxamento é comportamento correto (`n<3` é muito ruído para uma mediana confiável). Teste reescrito; adicionei segundo teste que verifica que com N≥3 o ano é mantido.

---

## 15. Execução do sistema completo

### Setup uma única vez
```bash
pip install -r requirements.txt
# Opcional: canais de alerta
export TELEGRAM_BOT_TOKEN="123:ABC..."
export TELEGRAM_CHAT_ID="-100123456"
export DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/..."
```

### Pipeline de descoberta + monitor + inteligência
```bash
# dia 0 — descobrir seed
python discover_links.py hilux diesel
python discover_links.py iphone 13
python discover_links.py playstation 5

# dia 0..∞ — monitor em loop (inclui todo o pipeline de inteligência)
python monitor.py --from-db --interval 21600 --concurrency 3
#   → extrai, reconcilia, grava price_history
#   → recomputa market_value
#   → recomputa opportunity_score
#   → roda new_listing_detector e duplicate_detector
#   → envia alertas via Telegram/Discord
#   → log estruturado em logs/monitor.log
```

### Comandos standalone
```bash
# intelligência sob demanda (sem re-fetchar)
python market_value.py
python opportunities.py
python new_listing_detector.py --hours 6
python duplicate_detector.py
python alerts.py --dry-run

# healthcheck do parser (continuação da v3)
python parser_health.py

# debug de um anúncio
python extract_item.py 2015275022700246 --debug-html

# analytics / stats
python analytics.py hilux iphone

# testes
pytest tests/                     # 91 testes, ~0.6s
pytest tests/test_market_value.py -v
```

### Dashboard
```bash
uvicorn web:app --reload --port 8000
```

Rotas:
- `/` — listings (500 últimos)
- `/stats` — métricas, heatmap por keyword, distribuição global
- `/top-deals` — top 50 por `opportunity_score`, com cores por severidade
- `/opportunities` — flags heurísticas (filtrável por rule)
- `/explorer?q=...` — busca por substring
- `/item/{id}` — detalhe com gráfico de histórico de preço (Chart.js)
- `/api/stats` · `/api/price_history/{id}` · `/api/price_distribution` · `/api/price_heatmap`

---

## 16. Resumo de arquivos

**Novos (v4):**
- [title_normalizer.py](title_normalizer.py) — fundacional puro
- [market_value.py](market_value.py) — estimativa via comparáveis
- [new_listing_detector.py](new_listing_detector.py) — novos anúncios quentes
- [alerts.py](alerts.py) — Telegram + Discord webhooks
- [duplicate_detector.py](duplicate_detector.py) — clustering por similaridade
- [templates/top_deals.html](templates/top_deals.html) — ranking de ofertas
- [tests/test_title_normalizer.py](tests/test_title_normalizer.py)
- [tests/test_market_value.py](tests/test_market_value.py)
- [tests/test_duplicate_detector.py](tests/test_duplicate_detector.py)
- [tests/test_score.py](tests/test_score.py)
- [tests/test_alerts.py](tests/test_alerts.py)
- [tests/test_new_detector.py](tests/test_new_detector.py)

**Modificados (v4):**
- [db.py](db.py) — colunas `estimated_market_value`/`discount_percentage`/`opportunity_score`/`cluster_id`, migração idempotente, índices v4 pós-migração
- [opportunities.py](opportunities.py) — `compute_score()` + `score_all_listings()`
- [monitor.py](monitor.py) — `_run_intelligence_pipeline()` ao fim de cada passada
- [web.py](web.py) — rotas `/top-deals`, `/api/price_heatmap`
- [templates/base.html](templates/base.html) — nav com Top Deals
- [templates/stats.html](templates/stats.html) — chart de heatmap
- [tests/test_db.py](tests/test_db.py) — cobertura de schema v4 + migration

---

## 17. Escala, qualidade de dados e precificação (v5)

A versão 5 foca em três coisas: fazer o pipeline de inteligência escalar (índice invertido), melhorar a qualidade dos dados que entram (parser de preço unificado, outliers), e preparar o terreno para decisões mais informadas (score dinâmico, fraud, sales velocity, trends).

### 17.1 Novas colunas em `listings`

Aplicadas via migração idempotente em [db.py:_migrate_columns](db.py):

| Coluna | Tipo | Preenchida por |
|---|---|---|
| `duplicate_group_id` | INTEGER | `duplicate_detector.cluster_all()` — v5 migra daqui (antes usava `cluster_id`) |
| `price_outlier`      | INTEGER (0/1) | `outlier_detector.detect_outliers()` |
| `fraud_risk_score`   | INTEGER (0–100) | `fraud_detector.scan()` |

`cluster_id` agora pertence a `listing_cluster.py` (clustering loose, DBSCAN-ish). Bancos v4 que tinham cluster_id de duplicates serão sobrescritos na próxima passada — o valor é regenerado de todo jeito, e as duplicatas continuam marcadas (em `duplicate_group_id`). Dashboard [templates/top_deals.html](templates/top_deals.html) continua referenciando `cluster_id` — agora significa "grupo de similares", que é semanticamente mais útil para navegação.

### 17.2 `price_normalizer.py` — parser canônico único

Todo o projeto agora delega parsing de preço para [price_normalizer.parse()](price_normalizer.py). Cobre:

```
"R$ 1.234,56"   → 1234.56      (BR decimal)
"R$ 185.000"    → 185000.0     (BR milhar sem decimal)
"R$185000"      → 185000.0     (sem espaço)
"185000"        → 185000.0     (puro)
"185k" / "185K" → 185000.0     (sufixo k)
"1.5k" / "2,5k" → 1500 / 2500
"185 mil"       → 185000.0
"2 milhões"     → 2000000.0
"2,5 mi"        → 2500000.0
"1234.56"       → 1234.56      (intl decimal)
```

Heurística sem perda de info: vírgula é sempre decimal BR; ponto é ambíguo e só vira separador de milhar quando há múltiplos pontos OU o último grupo tem exatamente 3 dígitos. `analytics._to_float` agora delega para `price_normalizer.parse` — uma única fonte de verdade. 21 testes em [tests/test_price_normalizer.py](tests/test_price_normalizer.py) cobrindo cada formato e degenerate.

### 17.3 `ComparablesIndex` — market_value escalável

Antes da v5, `find_comparables()` varria a lista inteira para cada listing → O(N²) total no `recompute_all()`. Inviável acima de alguns milhares.

Agora em [market_value.ComparablesIndex](market_value.py):

```python
class ComparablesIndex:
    def __init__(self, items):
        self.items_by_id    = {it.id: it for it in items}
        self.by_brand       = {brand → [PricedItem, ...]}
        self.by_brand_year  = {(brand, year) → [PricedItem, ...]}
        self.by_token       = {token → set(ids)}      # ← índice invertido
        self._cache_comps   = {}                      # memoização intra-run
```

Cascata de busca agora é:
1. **brand+year** → hit direto no dicionário `by_brand_year[(b, y)]`
2. **brand** → hit direto em `by_brand[b]`
3. **Jaccard fallback** → candidatos = união dos `by_token[t]` para cada token do item; só itera sobre esses. Por exemplo, se só 5% dos listings compartilham algum token com o item alvo, o loop é 20× menor.

Complexidade prática: **O(N × k × c)** onde k = tokens médio por item, c = candidatos médio por token. Para N=10k isso cai da ordem de 10^8 comparações para ~10^6.

Também adicionei `exclude_outliers=True` como parâmetro default em `recompute_all()` — o pool de comparáveis ignora itens marcados como outlier (preço contaminado), mas os outliers ainda recebem estimativa vs. pool limpo, então você vê o desconto real deles.

6 testes em [test_comparables_index.py](tests/test_comparables_index.py) — índice, buckets, cascata, memoização, fallback Jaccard via invindex.

### 17.4 `outlier_detector.py` — IQR por grupo

Para cada listing com preço, agrupa por marca (ou por token lexicográfico principal se não houver marca). Para grupos com ≥ 5 itens, calcula Q1/Q3/IQR e marca como outlier todo item fora de `[Q1 − 1.5·IQR, Q3 + 1.5·IQR]`. Persiste em `listings.price_outlier`.

Detecta tanto "caros demais" (digitação errada: "R$ 999.999.999") quanto "baratos demais" (isca de golpe: "R$ 100 num iPhone"). O fraud_detector depois usa isso + discount vs. estimated_market_value para refinar.

**Integração com market_value:** outlier roda **antes** de market_value no pipeline, então a mediana do pool já sai limpa. Isso quebra o loop "outlier contamina estimativa → estimativa errada → mais outliers". 2 testes com DB isolado em [test_outlier_detector.py](tests/test_outlier_detector.py).

### 17.5 `listing_cluster.py` — clustering loose (DBSCAN-ish via Union-Find)

Critério de conexão ([_connectable](listing_cluster.py)):
- Jaccard(tokens) ≥ 0.5 (mais loose que o 0.7 do duplicate_detector)
- Marca compatível (se ambos tiverem)
- Anos dentro de `YEAR_TOLERANCE = 2` (se ambos tiverem)

Usa `ComparablesIndex.by_token` para prefiltrar candidatos ao invés de O(n²) bruto — só itera sobre itens que compartilham pelo menos 1 token com o alvo. Conectividade propaga via Union-Find, produzindo componentes conectadas como cluster_id.

Diferença conceitual vs. duplicate_detector:
- **listing_cluster** (loose): "Hilux SRV 2013 diesel" ~ "Hilux SR 2014 diesel" → mesma família de produto
- **duplicate_detector** (tight): jaccard ≥ 0.7 + preço ±10% + mesma cidade → provável repost do mesmo anúncio

6 testes em [test_listing_cluster.py](tests/test_listing_cluster.py).

### 17.6 `fraud_detector.py`

Heurísticas somadas, score 0–100 em `listings.fraud_risk_score`:

| Regra | Peso | Condição |
|---|---|---|
| `absurdly_cheap` | 25 | preço < 30% do `estimated_market_value` |
| `few_images` | 20 | < 2 imagens (meio-peso se sem payload) |
| `short_description` | 15 | descrição ausente ou < 20 chars |
| `short_title` | 10 | título com < 3 palavras |
| `generic_title` | 10 | < 2 tokens úteis (só stopwords) |
| `no_location` | 10 | `current_location` NULL |
| `huge_discount_plus_urgency` | 15 | desconto > 40% E keyword de urgência |

Score ≥ 50 é destacado como "high risk" no `/market-insights`. 10 testes em [test_fraud_detector.py](tests/test_fraud_detector.py).

### 17.7 `sales_velocity.py` — tempo até remoção

Calcula, para listings removidos, `(removed_at - first_seen_at)` em dias. Gera:

- **Global**: mean, median, p25, p75, min, max
- **Por token**: mesmas métricas agrupadas por token do título

Ordenação default do "top por token" é por volume desc, median asc — mostra primeiro os nichos que tanto têm dados quanto vendem rápido.

**Aviso honesto sobre ground truth:** "removido" pode ser vendido, cancelado ou moderado. Em agregado o sinal ainda é útil (liquidez média por categoria), mas não é prova de venda. Documentado no topo do módulo.

Exposto no `/market-insights` via card + tabela por token. CLI `python sales_velocity.py [--token iphone] [--json]`.

### 17.8 `score_optimizer.py` — pesos dinâmicos

Substitui pesos fixos do `opportunity_score` por pesos otimizados contra histórico.

**Ground truth:** listing removido em < 7 dias = "fast" (provável venda). Listings ativos não entram (sem label).

**Algoritmo (lift estatístico):**
1. Para cada sinal do `compute_score` (`discount_big`, `discount_mid`, etc.), calcula:
   - `P(fast | signal fired)` — frequência de fast entre os que dispararam o sinal
   - `base_rate = P(fast)` — frequência global
   - `lift = P(fast | signal) / base_rate`
2. Se lift > 1, o sinal *realmente* separa fast de slow; peso sobe proporcionalmente
3. Novo peso = `clamp(default × lift, 5, 60)`

**Persistência:** [config/score_weights.json](config/score_weights.json) (criado sob demanda). [opportunities.py:reload_weights()](opportunities.py) lê esse arquivo no startup e a cada passada do monitor. Se o arquivo não existe ou é inválido, usa `DEFAULT_SCORE_WEIGHTS` embutido — degrade gracioso.

**Guarda contra feedback loop:** os sinais `recent<2h` e `short_desc` são mantidos em default weight — são transientes ou sem evidência disponível, otimizar contra eles seria circular.

CLI: `python score_optimizer.py [--days 14] [--dry-run]`. Saída é uma tabela `signal | n_fired | lift | old → new`.

Exige `MIN_SAMPLES = 20` removidos rotulados — caso contrário devolve status `insufficient_data` e não grava.

### 17.9 `export_data.py`

Exporta listings filtrados em CSV / JSON / Parquet.

**Filtros combináveis:** `--keyword`, `--city`, `--min-score`, `--min-discount`, `--exclude-outliers`, `--limit`.

**Writers:**
- `csv` — `csv.DictWriter` da stdlib
- `json` — indentado, `default=str` para datetimes
- `parquet` — requer `pyarrow` (opcional); falha com mensagem clara se ausente

5 testes em [test_export_data.py](tests/test_export_data.py) cobrindo `_build_query` (SQL safety), `write_csv`, `write_json` e o filtro `exclude_outliers` contra DB seedado.

CLI:
```bash
python export_data.py --format csv --out exports/todos.csv
python export_data.py --format json --keyword hilux --min-discount 15 --out hilux.json
python export_data.py --format parquet --min-score 70 --out top.parquet
```

### 17.10 `profile_pipeline.py` — perfil de performance

Mede o tempo de cada etapa do pipeline sem fazer fetch. Opcionalmente inclui `extract_item.extract()` e `discover_links.discover()` se você passar `--extract` / `--discover`.

**Baseline medido em DB vazio:**
```
stage                                                 ms  status
-----------------------------------------------------------------
db.init_db                                         27.87  ok
outlier_detector.detect_outliers[dry]              15.96  ok
market_value.recompute_all[dry]                     2.55  ok
listing_cluster.cluster_all[dry]                    5.42  ok
duplicate_detector.cluster_all[dry]                 2.51  ok
fraud_detector.scan[dry]                            3.70  ok
opportunities.scan[dry]                             2.53  ok
opportunities.score_all_listings                    1.55  ok
new_listing_detector.scan[dry]                      2.95  ok
alerts.scan_and_alert[dry]                         44.64  ok
sales_velocity.compute_global                       4.31  ok
-----------------------------------------------------------------
TOTAL                                             113.99  ms
```

Interpretação: o overhead fixo do pipeline é ~114ms em DB vazio. À medida que o banco cresce, `market_value` e `listing_cluster` crescem como O(N×k×c) graças ao índice invertido, não O(N²). A etapa mais cara em absoluto é a conexão inicial e `alerts` (45ms) porque abre/fecha várias conexões SQLite — candidato a otimização futura (conexão única com `with connect()` externo).

Saída em JSON via `--json` para monitoramento em tempo real / integração com grafana.

### 17.11 `/market-insights`, `/price-trends`, `/outliers` — dashboard

**`/market-insights`** ([market_insights.html](templates/market_insights.html)):
- 6 cards: ativos/total, outliers, fraud ≥50, clusters, score médio, dias médios no mercado
- Tabela de velocidade por keyword (top 15 por volume, colorida por mediana de dias)

**`/price-trends`** ([price_trends.html](templates/price_trends.html)):
- Formulário com filtro por keyword + janela temporal (7/30/90/365 dias)
- Chart.js com linha de mediana de preço por dia + barras de contagem de snapshots (2 eixos)
- Consulta SQL agregada dinâmica sobre `price_history`

**`/outliers`** ([outliers.html](templates/outliers.html)):
- Listings com `price_outlier = 1`, ordenados por `|discount|` desc
- Mostra preço, estimado, desconto, score, fraud — oferece contexto completo para triagem manual

Nav atualizada em [templates/base.html](templates/base.html) com todos os links novos.

### 17.12 Integração no monitor

[monitor.py:_run_intelligence_pipeline](monitor.py) agora roda 9 etapas em ordem determinística:

```
1. outlier_detector.detect_outliers    → limpa dados antes da estimativa
2. market_value.recompute_all           → estimativa sem contaminação de outliers
3. listing_cluster.cluster_all          → cluster_id (loose)
4. duplicate_detector.cluster_all       → duplicate_group_id (tight)
5. fraud_detector.scan                  → precisa de estimated_market_value
6. opportunities.scan                   → heurísticas legadas (flags discretas)
7. opportunities.score_all_listings     → usa pesos recarregados
8. new_listing_detector.scan            → usa discount_percentage fresco
9. alerts.scan_and_alert                → só depois de score+discount prontos
```

`opportunities.reload_weights()` é chamado no início para pegar pesos atualizados pelo `score_optimizer` entre passadas. Cada etapa está em `try/except` isolado — falha numa loga ERROR mas não derruba as outras.

Linha de resumo no console:
```
  [pipeline] outliers=3  market=142  cluster=28  dup=5  fraud=11  opps=7  scored=200  new=2  alerts_tg=1  alerts_dc=1
```

### 17.13 Testes — 141 passando (50 novos em v5)

| Arquivo | Testes | Cobertura |
|---|---|---|
| `test_price_normalizer.py` | 21 | todos os formatos (k, mil, milhões, BR, intl, degenerate) |
| `test_comparables_index.py` | 6 | invindex, buckets, cache, fallback Jaccard |
| `test_fraud_detector.py` | 10 | cada regra isolada + combo urgência+desconto + clipping |
| `test_listing_cluster.py` | 6 | _connectable por critério, year tolerance, jaccard fallback |
| `test_outlier_detector.py` | 2 | DB isolado: grupos grandes flagam outlier, grupos pequenos não |
| `test_export_data.py` | 5 | _build_query, write_csv/json, filtro exclude_outliers |

Total: **91 → 141 testes**, execução em ~1s.

### 17.14 Impacto de performance (qualitativo)

| Aspecto | v4 | v5 |
|---|---|---|
| `recompute_all()` Jaccard fallback | O(N²) | O(N × k × c_avg) via invindex |
| `listing_cluster` N²? | — | mesmo invindex — candidatos prefiltrados |
| `duplicate_detector` | O(N²) puro | mantido (trade-off: tight criteria + limite N<5k) |
| Preço inconsistente entre módulos | 2 parsers diferentes | 1 parser canônico (`price_normalizer.parse`) |
| Outlier contamina estimativa | sim | não — `exclude_outliers=True` default |
| Pesos do score | fixos | opcional-dinâmicos via config JSON |
| Pipeline total (DB vazio) | ~150ms | ~114ms |

A otimização do market_value só se paga em escala: em DB com <100 listings, a lista linear é na verdade mais rápida por não ter overhead de dict. O invindex começa a compensar em ~500+ listings.

---

## 18. Execução — todos os comandos v5

```bash
# Setup único (mesma lista v4, sem mudanças)
pip install -r requirements.txt
pip install pyarrow        # opcional, para export em parquet

# Inicialização / migração automática de banco existente
python -c "import db; db.init_db()"

# --- Pipeline completo automático ---------------------------------
python monitor.py --from-db --interval 21600 --concurrency 3
#   → extrai, reconcilia, price_history
#   → outliers → market_value → cluster → dup → fraud
#   → opps → score → new → alerts
#   → carrega pesos de config/score_weights.json se existir

# --- Etapas individuais (dry-run ou grava) ------------------------
python outlier_detector.py
python market_value.py
python listing_cluster.py
python duplicate_detector.py
python fraud_detector.py
python new_listing_detector.py --hours 6

# --- Score dinâmico (rodar semanalmente) --------------------------
python score_optimizer.py --days 7
#   → lê histórico de removed_at, calcula lifts,
#     grava config/score_weights.json

# --- Sales velocity -----------------------------------------------
python sales_velocity.py                   # global + top 20 tokens
python sales_velocity.py --token iphone
python sales_velocity.py --json

# --- Exportação ---------------------------------------------------
python export_data.py --format csv --out exports/all.csv
python export_data.py --format json --keyword hilux --min-discount 15 \
       --exclude-outliers --out hilux.json
python export_data.py --format parquet --min-score 70 --out top.parquet

# --- Profile ------------------------------------------------------
python profile_pipeline.py
python profile_pipeline.py --extract 2015275022700246
python profile_pipeline.py --discover iphone --json

# --- Dashboard ----------------------------------------------------
uvicorn web:app --reload --port 8000
# rotas:
#   /                  listings
#   /stats             cards + heatmap
#   /top-deals         ordenado por score
#   /opportunities     flags por regra
#   /market-insights   cards + velocidade por keyword   ← NOVO
#   /price-trends      linha temporal filtrada          ← NOVO
#   /outliers          preços absurdos                  ← NOVO
#   /item/{id}         detalhe + gráfico
#   /api/price_trends  JSON para chart                  ← NOVO

# --- Tests --------------------------------------------------------
pytest tests/                      # 141 testes, ~1s
pytest tests/test_comparables_index.py -v
pytest tests/test_price_normalizer.py -v
```

---

## 19. Arquivos v5 — resumo

**Novos:**
- [price_normalizer.py](price_normalizer.py)
- [outlier_detector.py](outlier_detector.py)
- [listing_cluster.py](listing_cluster.py)
- [fraud_detector.py](fraud_detector.py)
- [sales_velocity.py](sales_velocity.py)
- [score_optimizer.py](score_optimizer.py)
- [export_data.py](export_data.py)
- [profile_pipeline.py](profile_pipeline.py)
- [templates/market_insights.html](templates/market_insights.html)
- [templates/price_trends.html](templates/price_trends.html)
- [templates/outliers.html](templates/outliers.html)
- [tests/test_price_normalizer.py](tests/test_price_normalizer.py)
- [tests/test_comparables_index.py](tests/test_comparables_index.py)
- [tests/test_fraud_detector.py](tests/test_fraud_detector.py)
- [tests/test_listing_cluster.py](tests/test_listing_cluster.py)
- [tests/test_outlier_detector.py](tests/test_outlier_detector.py)
- [tests/test_export_data.py](tests/test_export_data.py)

**Modificados:**
- [db.py](db.py) — +3 colunas (`duplicate_group_id`, `price_outlier`, `fraud_risk_score`) + índices
- [market_value.py](market_value.py) — `ComparablesIndex` + `exclude_outliers` + delega para `price_normalizer`
- [analytics.py](analytics.py) — `_to_float` delega para `price_normalizer`
- [opportunities.py](opportunities.py) — `DEFAULT_SCORE_WEIGHTS` + `reload_weights()` a partir de `config/score_weights.json`
- [duplicate_detector.py](duplicate_detector.py) — escreve `duplicate_group_id` ao invés de `cluster_id`
- [monitor.py](monitor.py) — `_run_intelligence_pipeline` expandido de 6 para 9 etapas
- [web.py](web.py) — rotas `/market-insights`, `/price-trends`, `/outliers`, `/api/price_trends`
- [templates/base.html](templates/base.html) — nav com Insights/Trends/Outliers

---

## 20. Escala, qualidade e capacidade preditiva (v6)

Esta versão empurra o projeto em três direções: (a) armazenamento analítico via Parquet para suportar escala além dos limites práticos do SQLite, (b) detecção proativa de regressão do parser, e (c) camada preditiva (preço esperado, liquidez, reliability) para ir além das heurísticas determinísticas das versões anteriores.

### 20.1 Mudanças de schema

Migração idempotente em [db.py:_migrate_columns](db.py) adiciona **5 colunas em `listings`** e **2 tabelas novas**:

| Coluna / Tabela | Tipo | Quem preenche |
|---|---|---|
| `listings.current_seller` | TEXT | `monitor.reconcile()` a partir de `Listing.seller_name` |
| `listings.predicted_price` | REAL | `price_model.train_and_predict()` |
| `listings.price_gap` | REAL | idem (= predicted − current) |
| `listings.liquidity_score` | INTEGER | `liquidity_model.score_all()` |
| `listings.seller_reliability_score` | INTEGER | `seller_patterns.scan()` denormaliza |
| Tabela `parser_health_history` | — | `parser_regression_detector.detect_regression()` |
| Tabela `seller_stats` | — | `seller_patterns.scan()` |

Três índices novos (`idx_listings_seller`, `idx_listings_liquidity`, `idx_listings_gap`) para sustentar ordenação do dashboard.

**Reutilização do padrão de migração:** `PRAGMA table_info` → ALTER TABLE ADD COLUMN só para o que falta. Bancos v5 evoluem automaticamente sem downtime ou perda. Testado em [test_db.py:test_migration_adds_missing_columns](tests/test_db.py).

### 20.2 `data_lake.py` — camada híbrida SQLite / Parquet

SQLite continua sendo a fonte canônica para ingestão em tempo real (o monitor escreve aqui). Parquet é um espelho otimizado para análise histórica.

**Layout em disco:**
```
data_lake/
├── listings/listings.parquet           (snapshot atual, sobrescrito)
├── snapshots/dt=YYYY-MM-DD/snapshots.parquet   (append-only particionado)
├── price_history/dt=YYYY-MM-DD/price_history.parquet
└── _sync_state.json                    (watermark do último sync)
```

**Sync incremental via watermark:** `sync_state.last_snapshot_at` guarda o `fetched_at` do último snapshot sincronizado. Próxima chamada só lê `WHERE fetched_at > watermark`, particiona por dia e escreve no parquet do dia certo. Deduplicação por id quando rewriting partição parcial.

**API:**
- `sync_parquet(full=False)` — incremental por default; `full=True` rewritea tudo
- `load_dataset(name)` — `name ∈ {listings, snapshots, price_history}` → `list[dict]`
- `query_dataset(name, min_col=..., max_col=..., eq_col=..., contains_col=...)` — filtros simples

**Dependência:** `pyarrow` é opcional. Sem ela, `data_lake` importa mas levanta `ImportError` claro com mensagem "pip install pyarrow" no primeiro uso de `sync_parquet()` / `load_dataset()`. Isso mantém o resto do projeto rodando mesmo sem pyarrow instalado.

CLI:
```bash
python data_lake.py sync         # incremental
python data_lake.py sync --full  # rewrite completo
python data_lake.py info         # estado do lake (bytes, arquivos, watermark)
```

### 20.3 `parser_regression_detector.py` — detecção automática de quebra

A v3 introduziu `parser_health.py` como comando manual. v6 automatiza o monitoramento:

1. Roda amostra via `parser_health._collect()` + `build_report()`
2. Persiste resultado em `parser_health_history` (tabela nova)
3. Compara ok_rate, og_rate, relay_rate, jsonld_rate contra a **média móvel dos últimos 10 reports**
4. Se qualquer métrica caiu `≥ 20 pontos percentuais` vs baseline → marca como regressão
5. Grava evento `parser_break` em `events` (via listing sentinel `__system__`)

**Listing sentinel `__system__`:** o schema de `events` tem `FOREIGN KEY(listing_id) REFERENCES listings(id)`, então eventos de sistema precisam de um listing dummy. `_ensure_system_listing()` faz `INSERT OR IGNORE` com `id='__system__', last_status='system'`. Bug caçado pelos testes v6 (ver 20.14).

**Baseline insufficient:** se há menos de 3 reports históricos, a função retorna `(False, ["baseline_insufficient"])` sem emitir alerta. Evita disparar regressão no primeiro dia.

CLI:
```bash
python parser_regression_detector.py                # sample 10 + grava + detecta
python parser_regression_detector.py --from-cache   # offline, usa html_cache/
python parser_regression_detector.py --history-only # lista histórico sem nova coleta
```

Integrar num cron diário é o uso pretendido. O código loga o verdict no `fb_search.parser_regression` logger e retorna exit code 2 se regressão detectada — fácil de hookar em alerts externos.

### 20.4 `comparables_cache.py` — persistência do índice

O `ComparablesIndex` da v5 vive em memória. Para runs repetidos sobre a mesma população, persistir o pool de `PricedItem`s evita o overhead de fetch + parse.

**Estratégia de invalidação:** fingerprint determinístico via SHA-256 sobre `(id, price)` ordenado por id. Comparação é barata — ~O(N) scan só lendo id e preço do SQL, sem materializar objetos. Se o fingerprint do DB bate com o do cache, carrega pickle; senão rebuilda.

```python
from comparables_cache import load_or_build
index, source = load_or_build()  # source ∈ {'cache', 'fresh', 'db_empty'}
```

**Trade-off documentado:** em populações pequenas (< 1k listings), o overhead de pickle load > scan SQL direto. O ganho aparece em 10k+ e cresce linearmente. Testes cobrem roundtrip, invariância a ordem, sensibilidade a mudança de preço.

CLI:
```bash
python comparables_cache.py build       # força rebuild
python comparables_cache.py info        # mostra fingerprint DB vs cache
python comparables_cache.py invalidate  # remove
```

### 20.5 `price_model.py` — preço esperado via ML leve

**Dois backends em runtime:**

1. **sklearn (preferido, opcional)**
   - Features: `HashingVectorizer(n_features=2^12)` sobre o título (sem vocabulário fixo — robusto a títulos novos) + brand_id + year + token_count
   - Target: `log1p(price)` (variância do preço reduz em escala log, GBR converge melhor)
   - Modelo: `GradientBoostingRegressor(n_estimators=80, max_depth=4)`
   - Treina em todos os listings ativos não-outlier com preço > 0
   - `predict() → expm1()` volta pra escala original

2. **fallback (stdlib)**
   - kNN sobre `ComparablesIndex`: `predicted_price = median(prices_comparables)`
   - Idêntico ao `estimated_market_value` do `market_value`, mas exposto com API consistente
   - Usado quando sklearn não está instalado OU quando `--backend fallback`

**Saída:** atualiza `listings.predicted_price` e `listings.price_gap = predicted − current` para todos os itens cobertos. Gap positivo = preço atual abaixo do que o modelo esperava → candidato a deal.

Detecção de backend em runtime ([_has_sklearn](price_model.py)): `try: import sklearn` — degrade gracioso. Se sklearn estiver instalado via `pip install scikit-learn` o backend é automaticamente promovido.

**MIN_TRAINING_SAMPLES = 30** — abaixo disso retorna `status='insufficient_data'` sem treinar (evita modelos inúteis). Testado em [test_price_model.py](tests/test_price_model.py) com DB seedado.

Limitação conceitual documentada: como as labels são as próprias prices que queremos prever, o modelo efetivamente "suaviza" o ruído — não é forecast de futuro, é um smoother do presente contra features. O `price_gap` ganha sentido porque listings dentro do mesmo grupo de features mas com preço anômalo se destacam.

### 20.6 `liquidity_model.py` — probabilidade de venda rápida

Combinação linear com saturação de 5 sinais, peso fixo:

| Sinal | Peso | Saturação |
|---|---|---|
| `discount` vs mercado | 30 | 40% |
| `opportunity_score` | 25 | 80 |
| `desc_len` (payload) | 10 | 200 chars |
| `cluster_size` | 10 | 10 membros |
| `token_velocity` (do sales_velocity) | 25 | ≤ 7 dias mediana |

Score 0–100 persistido em `listings.liquidity_score`. Função [compute_liquidity](liquidity_model.py) é pura (sem DB), testada isoladamente com cada sinal em isolamento e em combinação.

**Token velocity** é um índice pré-computado: para cada token observado em listings removidos, calcula a mediana de dias até remoção, normaliza para `[0, 1]` (1 = muito rápido). Cada listing busca o **melhor** (mais rápido) entre seus tokens. Isso captura o sinal "itens com keyword X historicamente vendem em Y dias".

**Não é ML:** optei por função linear transparente ao invés de logistic regression, pelos mesmos motivos do score_optimizer — sem ground truth confiável de "vendido vs. cancelado", a inferência de pesos fica circular. Se no futuro tivermos dados mais limpos, dá para trocar pela calibração automática sem mudar a API pública.

Bug corrigido nos testes: `cluster_size` sem `cluster_id` definido contribuía ~1 ponto baseline. Fix: pular a contribuição quando `cluster_id is None`.

### 20.7 `seller_patterns.py` — análise de vendedores

Requer que o monitor popule `listings.current_seller` (feito em [monitor.reconcile](monitor.py) a partir de `Listing.seller_name`). Quando o FB não expõe seller sem login (geralmente), o listing não entra nesta análise.

**Métricas por vendedor** persistidas em tabela `seller_stats`:
- `total_listings`, `active_listings`, `removed_listings`
- `duplicate_count` — listings deste vendedor que caem num `duplicate_group_id` onde há outros dele (sinal de repost)
- `avg_price`, `avg_opportunity`, `avg_fraud`
- `reliability_score` (0–100)

**Heurística de reliability** em [_compute_reliability](seller_patterns.py):
- Começa em 100
- `-20` se `dup_count/total > 0.30` (muita duplicata)
- `-10` se `total > 50` (flooder)
- `-25` se `avg_fraud > 50`
- `-15` se `removed/total > 0.70` com total > 5 (churn alto)

Score final é denormalizado em `listings.seller_reliability_score` para cada listing, permitindo ordenação direta no dashboard.

8 testes em [test_seller_patterns.py](tests/test_seller_patterns.py) cobrindo cada penalidade isolada, combinações, piso de 0.

### 20.8 `deal_simulator.py` — simulação de ROI

Simula "comprar" listings filtrados por critério (score, desconto, keyword, cidade), respeitando um capital limite. Calcula:

- `investment`: total gasto (respeita capital)
- `estimated_value`: soma dos `estimated_market_value` correspondentes
- `gross_profit`: `estimated_value − investment`
- `hit_rate`: fração dos escolhidos que tem evento `removed` (proxy para "teria vendido")
- `expected_roi_pct = gross_profit × hit_rate / investment × 100`

**Limitações declaradas no topo do módulo:** "removed" é proxy otimista para venda; sem custo de frete/tempo/taxas; o resultado é um **teto superior**. Documentar limitações honesto é mais útil que esconder — o usuário decide quanto peso dar.

Testes com DB isolado verificam filtro por score, limite de capital, computo de hit_rate e filtro por keyword.

### 20.9 `weekly_report.py` — relatório HTML standalone

Gera `reports/report_YYYY-MM-DD.html` + cópia em `reports/latest.html`. Usa Jinja-less string templating (sem dependência de Jinja2 — report é autocontido e serve para enviar por email) com Bootstrap via CDN.

Seções:
1. Resumo (cards: ativos, novos, removidos, price_changes)
2. Top deals novos da semana (top 15 por opportunity_score)
3. Maiores quedas de preço (diff entre old/new value em events de `price_change`)
4. Clusters mais ativos (≥ 2 novos no período)
5. Tokens com maior desconto médio (mínimo 5 listings por token)
6. Eventos relevantes (parser_break, alert_sent, new_opportunity, opportunity_flag)

CLI:
```bash
python weekly_report.py             # últimos 7 dias
python weekly_report.py --days 30
python weekly_report.py --out custom.html
```

Modo pretendido de uso: cron semanal + envio por email. Integra bem com qualquer MTA local.

### 20.10 `fast_analytics.py` — analytics acelerado via Parquet

Mesmas estatísticas do `analytics.py`, mas com backend polars / pandas / stdlib.

**Detecção automática:** `polars → pandas → stdlib` na ordem. Pode ser forçado via `--backend`. Os dois primeiros leem direto do parquet gerado por `data_lake.sync_parquet()` — ordens de magnitude mais rápido em N grande que os `statistics` + loop da stdlib.

**Fonte canônica única de parsing de preço:** todos os três backends chamam `price_normalizer.parse` via `map_elements` (polars) / `.map()` (pandas) / loop (stdlib). Garante consistência dos resultados independente do backend.

CLI:
```bash
python fast_analytics.py hilux iphone
python fast_analytics.py --backend polars hilux
python fast_analytics.py --backend stdlib hilux    # lê do SQLite (não requer sync)
```

Se o backend escolhido precisa do parquet e ele não existe, erro claro orientando a rodar `python data_lake.py sync` antes.

### 20.11 Dashboard — 3 rotas novas

- **`GET /liquidity`** ([liquidity.html](templates/liquidity.html)) — listagem ordenada por `liquidity_score desc`, com badge colorida (verde ≥70, amarelo ≥40)
- **`GET /predicted-price`** ([predicted_price.html](templates/predicted_price.html)) — ordenada por `price_gap desc`, mostra preço atual vs. mediana do market_value vs. previsão do modelo ML, lado a lado. Gap positivo em verde.
- **`GET /sellers`** ([sellers.html](templates/sellers.html)) — ranking via `seller_stats` com badge de reliability colorida

Nav atualizada em [templates/base.html](templates/base.html) com os 3 links novos. Dashboard agora tem 9 rotas HTML principais + 5 APIs JSON.

### 20.12 Pipeline do monitor — 12 etapas

[monitor.py:_run_intelligence_pipeline](monitor.py) roda em ordem determinística (cada etapa em `try/except` isolado):

```
1. outlier_detector          (limpa pool)
2. market_value              (precisa de pool limpo)
3. listing_cluster           (reutiliza ComparablesIndex)
4. duplicate_detector        (tight grouping)
5. fraud_detector            (precisa de estimated_market_value)
6. opportunities.scan        (heurísticas discretas)
7. score_all_listings        (com pesos recarregados do config)
8. seller_patterns           (denormaliza reliability em listings)  ← novo
9. price_model               (treina e aplica)                       ← novo
10. liquidity_model          (consome discount, score, cluster, velocity)  ← novo
11. new_listing_detector     (usa discount fresco)
12. alerts                   (último — só envia quando score está pronto)
```

`reload_weights()` chamado no início para pegar o último `config/score_weights.json` gerado pelo score_optimizer. Cada falha loga ERROR mas não derruba as outras — o monitor sempre tenta progredir.

Linha de resumo no console agora:
```
[pipeline] outliers=3 market=142 cluster=28 dup=5 fraud=11 opps=7 scored=200 sellers=45 pm=198 liq=200 new=2 alerts=2
```

### 20.13 Performance baseline

Medido com DB vazio (overhead fixo do pipeline):

```
v5: 113.99 ms
v6: 119.34 ms   (+5.35ms, +4.7%)
```

O delta é esperado: migração aplica 5 ALTER TABLE + cria 2 tabelas novas na primeira passada. Após o primeiro init_db, custo adicional é ~0 (`CREATE IF NOT EXISTS` + `PRAGMA table_info` são near-instant).

Com dados reais, o gargalo passa a ser `sales_velocity.compute_by_token()` (chamado dentro do liquidity_model) que faz full scan de listings removidos. Para N > 50k isso vira candidato a otimização com cache dedicated + invalidação por evento `removed`.

### 20.14 Testes — 179 passando (38 novos v6)

| Arquivo | Testes | Cobertura |
|---|---|---|
| `test_liquidity_model.py` | 8 | cada sinal isolado + soma máxima + clipping |
| `test_seller_patterns.py` | 8 | cada penalidade + stacking + piso |
| `test_comparables_cache.py` | 7 | fingerprint estabilidade/invariância + roundtrip + invalidate |
| `test_deal_simulator.py` | 4 | filtros de score/capital/keyword + hit_rate |
| `test_price_model.py` | 3 | insufficient data + fallback backend + dry-run |
| `test_parser_regression.py` | 4 | baseline insuficiente + healthy + og drop + ok drop |
| `test_weekly_report.py` | 4 | collect + drops + render + DB vazio |

Total: **141 → 179 testes**, ~1.8s.

### 20.15 Bugs reais encontrados pelos testes (v6)

1. **`liquidity_model.compute_liquidity` dava baseline de ~1 ponto para listings sem cluster_id.** Acontecia porque `_scale(1, 10) × 10 = 1.0`, aplicado quando o fallback `cs = cluster_sizes.get(cid, 1) if cid is not None else 1` rodava com `cid=None`. Fix: pular a contribuição inteira quando `cluster_id is None`. Isso também casa semanticamente melhor com "sinal ausente" vs "sinal presente baixo".

2. **`parser_regression_detector` violava FK em `events.listing_id`.** Tentava inserir evento `parser_break` com `listing_id='*'`, mas o FK para `listings(id)` exige que exista. Fix: criar um listing sentinel `__system__` via `INSERT OR IGNORE` na mesma transação. Isso preserva o FK (útil para limpeza por cascade) e dá um anchor natural para futuros eventos de sistema.

---

## 21. Execução — comandos v6

### Setup
```bash
pip install -r requirements.txt

# Opcionais (ativam funcionalidades extras):
pip install scikit-learn scipy     # price_model backend sklearn (preferido)
pip install pyarrow                # data_lake + export_data parquet
pip install polars                 # fast_analytics (polars > pandas > stdlib)
```

### Pipeline contínuo (o monitor chama tudo isso automaticamente)
```bash
python monitor.py --from-db --interval 21600 --concurrency 3
```

### Comandos novos v6
```bash
# Armazenamento analítico
python data_lake.py sync                # SQLite → Parquet incremental
python data_lake.py sync --full         # rewrite
python data_lake.py info

# Regressão do parser (cron diário recomendado)
python parser_regression_detector.py
python parser_regression_detector.py --history-only
python parser_regression_detector.py --from-cache

# Cache persistente de comparáveis
python comparables_cache.py info
python comparables_cache.py build
python comparables_cache.py invalidate

# Modelo de preço
python price_model.py                   # auto backend (sklearn se disponível)
python price_model.py --backend fallback --dry-run

# Liquidez
python liquidity_model.py

# Padrões de vendedor
python seller_patterns.py               # recomputa + grava
python seller_patterns.py --top 20      # só imprime ranking

# Simulação de investimento
python deal_simulator.py --capital 50000 --min-score 70
python deal_simulator.py --capital 20000 --min-discount 25 --show-picks 10
python deal_simulator.py --capital 100000 --keyword hilux --json

# Relatório semanal
python weekly_report.py                 # últimos 7 dias → reports/latest.html
python weekly_report.py --days 30

# Fast analytics (precisa de sync antes para backends parquet)
python fast_analytics.py hilux iphone
python fast_analytics.py --backend polars hilux
python fast_analytics.py --backend stdlib hilux    # lê SQLite direto
```

### Dashboard (3 rotas novas)
```bash
uvicorn web:app --port 8000
#   /liquidity          ranking por liquidity_score
#   /predicted-price    gap entre preço atual e previsão ML
#   /sellers            ranking por reliability
```

### Testes
```bash
pytest tests/               # 179 passando
pytest tests/test_liquidity_model.py -v
pytest tests/test_price_model.py -v
```

---

## 22. Arquivos v6 — resumo

**Novos módulos:**
- [data_lake.py](data_lake.py) — SQLite ↔ Parquet com watermark incremental
- [parser_regression_detector.py](parser_regression_detector.py)
- [comparables_cache.py](comparables_cache.py)
- [price_model.py](price_model.py) — backend sklearn/fallback
- [liquidity_model.py](liquidity_model.py)
- [seller_patterns.py](seller_patterns.py)
- [deal_simulator.py](deal_simulator.py)
- [weekly_report.py](weekly_report.py)
- [fast_analytics.py](fast_analytics.py)

**Novos templates:**
- [templates/liquidity.html](templates/liquidity.html)
- [templates/predicted_price.html](templates/predicted_price.html)
- [templates/sellers.html](templates/sellers.html)

**Novos testes:**
- [tests/test_liquidity_model.py](tests/test_liquidity_model.py)
- [tests/test_seller_patterns.py](tests/test_seller_patterns.py)
- [tests/test_comparables_cache.py](tests/test_comparables_cache.py)
- [tests/test_deal_simulator.py](tests/test_deal_simulator.py)
- [tests/test_price_model.py](tests/test_price_model.py)
- [tests/test_parser_regression.py](tests/test_parser_regression.py)
- [tests/test_weekly_report.py](tests/test_weekly_report.py)

**Modificados:**
- [db.py](db.py) — 5 colunas novas em listings + 2 tabelas novas + 3 índices
- [monitor.py](monitor.py) — pipeline expandido de 9 → 12 etapas; grava `current_seller` no reconcile
- [web.py](web.py) — rotas `/liquidity`, `/predicted-price`, `/sellers`
- [templates/base.html](templates/base.html) — nav com os 3 links novos

---

## 23. Descoberta, análise geográfica e inteligência específica do Marketplace (v7)

A v7 reposiciona o projeto para dentro do contexto específico do Facebook Marketplace: extração geográfica, classificação por categoria, modelo apertado para veículos, detecção de reposts e análise de densidade de mercado por token. O pipeline do monitor foi expandido de 12 para 16 etapas.

### 23.1 Mudanças de schema

Migração idempotente em [db.py:_migrate_columns](db.py) adiciona **5 colunas** em `listings` e **2 tabelas** novas:

| Item | Tipo | Quem popula |
|---|---|---|
| `listings.city` | TEXT | `geo_coverage.apply_to_listings()` |
| `listings.state` | TEXT (sigla UF) | idem |
| `listings.category` | TEXT | `category_models.apply_classification()` |
| `listings.repost_count` | INTEGER NOT NULL DEFAULT 0 | `repost_detector.detect_reposts()` |
| `listings.fresh_opportunity_score` | INTEGER | `fresh_opportunity_detector.scan()` |
| Tabela `geo_coverage` | — | `geo_coverage.persist_coverage()` |
| Tabela `market_density` | — | `market_density.persist()` |

Cinco novos índices (`city`, `state`, `category`, `repost_count`, `fresh_opportunity_score`) para ordenações do dashboard. Tudo compatível com bancos v6 existentes.

### 23.2 `geo_coverage.py` — parse + score de cobertura

**Parser** ([parse_location](geo_coverage.py)): best-effort sobre os formatos comuns do Marketplace BR — `"São Paulo, SP"`, `"Rio - RJ"`, `"Campinas, São Paulo"`, `"Brasília"`, etc.

Retorna `(city|None, state|None)`. A sigla é sempre 2 letras (`SP`, `RJ`, ...). Full-name de estado é aceito apenas quando NÃO é o primeiro elemento da lista — sem essa regra, `"São Paulo, SP"` virava `(state="SP", city="SP")` porque "São Paulo" também bate no mapa de nomes completos. Bug caçado pelos testes (ver 23.15).

**Coverage score** (0..100) combina 3 componentes em [_compute_coverage_score](geo_coverage.py):
```
vol    = min(50, 50 * log1p(active) / log1p(1000))     # volume log-escala
div    = min(30, 30 * log1p(distinct_tokens) / log1p(200))  # diversidade
fresh  = max(0, int(20 * (1 − days_since_last / 30)))  # freshness linear (0 após 30d)
score  = min(100, vol + div + fresh)
```

Três fases públicas:
1. `apply_to_listings()` — percorre `listings`, parsea `current_location`, grava `city`/`state`
2. `compute_coverage()` — agrega por `(city, state)`, calcula stats + score
3. `persist_coverage()` — grava em `geo_coverage` (overwrite completo)

Agrupadas em `run()`. Chamadas pelo monitor no início do pipeline.

### 23.3 `geo_heatmap.py` — camada de apresentação geo

Wrapper read-only sobre `geo_coverage` que serve o dashboard e exporta JSON. Não computa nada novo — é a divisão clara entre "como coletar" (geo_coverage) e "como mostrar" (geo_heatmap).

APIs:
- `top_cities_by_volume(limit)` / `top_cities_by_discount(limit)`
- `by_state()` — agregação por UF (n cidades, n ativos, desconto médio)
- `heatmap_dataset(limit)` — dict pronto para JSON

CLI pode imprimir em texto ou emitir JSON: `python geo_heatmap.py --by-state`, `python geo_heatmap.py --json`.

### 23.4 `category_models.py` — classificação rule-based

**4 categorias + other:** `vehicles`, `electronics`, `real_estate`, `furniture`. Vocabulários curados em constantes no topo do módulo, fáceis de estender.

Algoritmo em [classify](category_models.py):
1. Tokeniza o título (reutiliza `title_normalizer.tokens` que já remove stopwords)
2. Conta hits em cada vocabulário
3. Categoria com mais hits vence
4. Empate: ordem fixa `CATEGORY_PRIORITY = [vehicles, electronics, real_estate, furniture]`

Documentei explicitamente por que **não** uso sklearn aqui: o input é um título curto com alta variância lexical, e o vocabulário curado é tão eficiente quanto e infinitamente mais explicável. Se a precisão cair quando o mercado mudar, basta atualizar as listas de tokens.

`category_stats()` agrega por categoria (total, ativos, preço médio, desconto médio, liquidez média) — consumido pelo `market_report` e pela futura expansão do dashboard.

### 23.5 `vehicle_model.py` — cascata especializada

Para a categoria `vehicles`, o matching genérico de `market_value` é fraco porque perde nuances críticas (`diesel` ≠ `flex`, `2013` ≠ `2018`). Este módulo extrai features estruturadas e aplica uma cascata mais apertada.

[VehicleFeatures](vehicle_model.py):
```python
brand, model, year, fuel, transmission, engine, traction
```

- `brand` vem de `title_normalizer.extract_brand` (lista já existente de marcas conhecidas)
- `year` de `extract_year`
- `fuel` ∈ {diesel, gasolina, flex, etanol, gnv, eletrico, hibrido}
- `transmission` ∈ {automatica, manual, cvt, ...}
- `traction` ∈ {4x4, 4x2, awd, fwd, rwd}
- `engine` via regex `\d\.\d` (ex.: "1.0", "2.0")
- `model` via **whitelist curada** de modelos conhecidos (hilux, civic, onix, gol, cg, titan, ...). Whitelist é chave para evitar que tokens aleatórios virem "modelo".

Cascata de comparáveis em [find_vehicle_comparables](vehicle_model.py):
```
1. brand + model + year±1 + fuel
2. brand + model + year±2 + fuel
3. brand + model + year±2
4. brand + model + year±4
5. brand + model (qualquer ano, fallback)
```

Cada nível exige ≥ `MIN_COMPARABLES = 3`. Se um nível falha, cai para o próximo. `find_vehicle_comparables` devolve `[]` quando o target não tem marca/modelo reconhecidos — nesses casos, o `market_value` genérico segue no comando.

`apply_vehicle_valuation()` roda essa cascata **apenas** para listings com `category='vehicles'` e sobrescreve `estimated_market_value`/`discount_percentage` com a estimativa refinada. Deixa as outras categorias intocadas. Integrado no pipeline do monitor logo após `market_value.recompute_all()`.

### 23.6 `repost_detector.py` — padrão de republicação

Para cada listing removido A, procura listings novos B tais que:
- `A.current_seller == B.current_seller` (ou ambos None; nesse caso a barra é mais alta no título)
- `B.first_seen_at` está dentro de `REPOST_WINDOW_DAYS = 14` depois de `A.removed_at`
- `jaccard(tokens(A.title), tokens(B.title)) ≥ 0.7`
- `|A.price − B.price| / min ≤ 15%` (se ambos tiverem preço)

**Otimização:** bucketiza por seller antes da comparação bilateral. O loop vira `O(S × k²)` onde S é sellers distintos e k é anúncios por seller — muito menor que o `O(N²)` ingênuo para uma base de dezenas de milhares.

Incrementa `listings.repost_count` no listing **novo** (o B) e grava evento `repost_detected` com `new_value = "origin:{A.id}"` para rastrear. Dedup por origem já flagada: se o evento específico já existe, não reemite.

CLI: `python repost_detector.py --window-days 30 --dry-run`. Não está no pipeline do monitor (rode via cron diário — é pesado e benefit diminishing).

### 23.7 `listing_timeline.py` — história do anúncio

Reconstrói a timeline completa de um listing combinando 3 fontes:
1. `listings.first_seen_at` → evento `listing_created`
2. Tabela `events` completa (já contém `price_change`, `title_change`, `removed`, `reappeared`, `opportunity_flag`, `alert_sent`, `new_opportunity`, `fresh_opportunity`, `repost_detected`, `parser_break`)
3. Tabela `price_history` como pontos `price_point` (só onde não há um `price_change` correspondente no mesmo timestamp, evita duplicação)

Retorna `list[TimelineEntry(at, kind, description, details)]`, ordenado cronologicamente. Descrições humanas por `event_type` em dicionário `DESCRIPTIONS`.

Dashboard route `/anuncio-timeline/{id}` ([templates/listing_timeline.html](templates/listing_timeline.html)) renderiza a timeline com badges coloridas por tipo. API endpoint `/api/listing_timeline/{id}` para consumo programático.

CLI: `python listing_timeline.py 2015275022700246` ou `--json`.

### 23.8 `fresh_opportunity_detector.py` — janela de 30 min

Diferente do `new_listing_detector` (v4) que usa janela de 2h e critério qualitativo, este módulo tem janela de 30 minutos e produz score numérico 0..100.

Score em [compute_fresh_score](fresh_opportunity_detector.py):
```
age > 30min            → 0 (fora da janela)
discount > 30%         → +40
discount > 15% (mid)   → +20
liquidity ≥ 60         → +30
opportunity ≥ 70       → +20
popular_keyword (título)→ +10
clip em 100
```

Persistido em `listings.fresh_opportunity_score`. Emite evento `fresh_opportunity` (uma vez por listing, dedupado) quando score ≥ `EMIT_THRESHOLD = 60`.

Integrado no pipeline do monitor como penúltimo passo (antes de alerts) — assim os alertas podem disparar em cima do evento recém-emitido.

### 23.9 `market_density.py` — competição por token

Para cada token comum (≥ `MIN_COUNT = 5` listings), calcula:
```
total_listings, active_listings, removed_listings
removal_rate        = removed / total
avg_velocity_days   = mediana de (removed_at − first_seen_at)
competition_score   = volume_component + turnover_component
  onde:
    volume_component   = min(60, 60 * log1p(active) / log1p(200))
    turnover_component = int(40 * removal_rate)
```

**Interpretação do competition_score:**
- Alto = nicho concorrido MAS líquido (muita gente vendendo, muita gente comprando)
- Baixo = nicho raro (pouca oferta) OU mercado lento (pouca rotatividade)

Persistido em tabela `market_density` (overwrite completo a cada run). Dashboard em `/market-density` ([templates/market_density.html](templates/market_density.html)) mostra badge colorida (vermelho ≥75, amarelo ≥50, verde <50).

CLI: `python market_density.py --min-count 10 --top 30`.

### 23.10 `market_report.py` — relatório de mercado

Diferente do `weekly_report.py` (que é diário/semanal focado em mudanças recentes), o `market_report` consolida a visão MACRO:

1. Cards: ativos, cidades, categorias, tokens rastreados
2. Top cidades por volume + por desconto médio (de `geo_coverage`)
3. Stats por categoria (de `category_models.category_stats`)
4. Top tokens por competição (de `market_density`)
5. Top tokens por desconto médio (join on-the-fly entre market_density e discount_percentage via LIKE)
6. Fresh opportunities no período (eventos `fresh_opportunity`)
7. Reposts detectados no período (eventos `repost_detected`)

HTML standalone (Bootstrap via CDN, sem Jinja2 — autocontido para envio por email). `reports/market_report_YYYY-MM-DD.html` + `market_latest.html`.

### 23.11 Pipeline do monitor — 16 etapas

[monitor.py:_run_intelligence_pipeline](monitor.py):

```
 1. geo_coverage.run              ← NOVO: parse + score cobertura
 2. category_models.apply_classification ← NOVO: categoria para cada listing
 3. outlier_detector               (limpa pool para market_value)
 4. market_value.recompute_all     (genérico, todas as categorias)
 5. vehicle_model.apply_vehicle_valuation ← NOVO: refina vehicles
 6. listing_cluster
 7. duplicate_detector
 8. fraud_detector
 9. opportunities.scan
10. opportunities.score_all_listings
11. seller_patterns
12. price_model.train_and_predict
13. liquidity_model.score_all
14. new_listing_detector
15. fresh_opportunity_detector.scan ← NOVO: <30min + score fresco
16. alerts.scan_and_alert          (último)
```

Cada um em try/except isolado (falha não derruba as outras). Ordem importa:
- `geo_coverage` e `category_models` vêm ANTES porque `vehicle_model` depende de `category='vehicles'`
- `vehicle_model` vem DEPOIS de `market_value` (genérico) para sobrescrever só os vehicles
- `fresh_opportunity_detector` vem ANTES de `alerts` para que fresh flags sejam consideradas no envio

Os módulos mais pesados (`repost_detector`, `market_density`, `market_report`) **não** estão no pipeline — ficam como CLI para rodar via cron diário ou sob demanda.

Linha de resumo do console:
```
[pipeline] geo_cities=142 cats=1850 outliers=23 market=1700 vehicles=340 cluster=88
           dup=12 fraud=47 opps=27 scored=1850 sellers=320 pm=1800 liq=1850 new=5 fresh=2 alerts=2
```

### 23.12 Dashboard — 4 rotas novas

| Rota | Template | Função |
|---|---|---|
| `/geo-insights` | [geo_insights.html](templates/geo_insights.html) | Top cidades por volume/desconto + por estado |
| `/anuncio-timeline/{id}` | [listing_timeline.html](templates/listing_timeline.html) | Histórico completo do listing em timeline |
| `/fresh-deals` | [fresh_deals.html](templates/fresh_deals.html) | Oportunidades <30min ordenadas por score |
| `/market-density` | [market_density.html](templates/market_density.html) | Ranking de tokens por competition_score |

APIs JSON novas:
- `/api/listing_timeline/{id}` — timeline serializada

Nav em [base.html](templates/base.html) tem agora 12 links principais: Listings, Explorer, Stats, Top Deals, Opportunities, Insights, Trends, Outliers, Liquidity, Predicted, Sellers, Geo, Fresh, Density.

### 23.13 Performance baseline (DB vazio)

```
v5: 113.99 ms
v6: 119.34 ms   (+5ms)
v7: 138.86 ms   (+20ms)
```

Delta v6→v7 vem da migração (5 ALTER TABLE + 2 CREATE TABLE + 5 CREATE INDEX) que roda uma vez por processo. Em produção, após o primeiro init, custo recorrente do pipeline expandido é:
- `geo_coverage.apply_to_listings()` ~ O(N) scan de location strings
- `category_models.apply_classification()` ~ O(N × V) onde V é tamanho dos vocabulários
- `vehicle_model.apply_vehicle_valuation()` ~ O(K²) onde K = listings com category='vehicles' (muito menor que N)
- `fresh_opportunity_detector` ~ O(N_active) mas só processa os < 30min de idade

### 23.14 Testes — 232 passando (53 novos v7)

| Arquivo | Testes | Cobertura |
|---|---|---|
| `test_geo_coverage.py` | 12 | parse_location (todos os formatos + city-only + state-only) + coverage_score saturation |
| `test_category_models.py` | 9 | cada categoria + empate + ambiguidade lexical |
| `test_vehicle_model.py` | 9 | extract features + cascata apertada + fallbacks |
| `test_repost_detector.py` | 8 | _is_repost positivo/negativo por critério + _price_close |
| `test_fresh_opportunity.py` | 8 | cada sinal isolado + janela + naive timestamps |
| `test_market_density.py` | 7 | _compute_competition_score + DB isolado com agregação por token |

Total: **179 → 232 testes**, ~2.6s.

### 23.15 Bugs reais encontrados pelos testes (v7)

1. **`parse_location("São Paulo, SP")` retornava `("Sp", "SP")`.** O full-name mapping de estados contém `"são paulo" → "SP"`, então o primeiro elemento era interpretado como estado redundante, e o segundo ("SP") caía em `city_parts` e virava "Sp" após title case. Fix: `_canonical_state(token, allow_full_name=False)` no primeiro elemento da lista de partes. Abreviações 2-letras continuam sempre sendo aceitas em qualquer posição. Isso resolve também o caso "Rio de Janeiro, RJ, Brasil" que tinha o mesmo padrão.

2. **Teste de `classify("moto lenovo")` assumia tie 1-1.** O vocabulário `ELECTRONICS_TOKENS` inclui `"moto"` porque é abbr de Motorola (smartphones). Resultado: o input tinha 2 hits em electronics ("moto" + "lenovo") vs 1 em vehicles ("moto"), então electronics ganhava legitimamente — não era empate. Fix no teste, não no código: usar `"carro lenovo"` para um empate genuíno.

3. **Teste de `_compute_competition_score(active=1, removal_rate=1.0)` assumia 0 volume.** `log1p(1) = 0.693` (não zero), então volume_component vira ~7 e o total é 47, não 40. Fix no teste (expectativa correta).

Os dois últimos são ajustes de expectativa — documentam conhecimento ganho sobre o comportamento real. O primeiro é bug genuíno de lógica caçado só por ter teste com entrada ambígua real.

---

## 24. Execução — comandos v7

### Pipeline contínuo (todas as 16 etapas)
```bash
python monitor.py --from-db --interval 21600 --concurrency 3
```

### Comandos novos v7
```bash
# Geo
python geo_coverage.py                    # apply + compute + persist
python geo_coverage.py --apply-only       # só popula city/state
python geo_heatmap.py --top 25
python geo_heatmap.py --by-state
python geo_heatmap.py --json > heatmap.json

# Categorias
python category_models.py                 # classify + stats
python category_models.py --classify-only

# Veículos
python vehicle_model.py extract "Toyota Hilux SRV 2013 Diesel 4x4"
python vehicle_model.py apply              # recalcula estimativa p/ vehicles

# Reposts (cron diário recomendado)
python repost_detector.py --window-days 14
python repost_detector.py --dry-run

# Timeline
python listing_timeline.py 2015275022700246
python listing_timeline.py 2015275022700246 --json

# Fresh opportunities (no pipeline também)
python fresh_opportunity_detector.py --minutes 30 --threshold 60

# Densidade de mercado (cron diário recomendado)
python market_density.py --min-count 10 --top 30

# Relatório de mercado
python market_report.py --days 7          # → reports/market_latest.html
python market_report.py --days 30
```

### Dashboard — 4 rotas novas
```bash
uvicorn web:app --port 8000

#   /geo-insights                cobertura geo + top cidades
#   /anuncio-timeline/{id}       history completa
#   /fresh-deals                 oportunidades recentes
#   /market-density              competição por token
#   /api/listing_timeline/{id}   JSON
```

### Tests
```bash
pytest tests/                              # 232 passing
pytest tests/test_vehicle_model.py -v
pytest tests/test_geo_coverage.py -v
```

---

## 25. Arquivos v7 — resumo

**Novos módulos (10):**
- [geo_coverage.py](geo_coverage.py)
- [geo_heatmap.py](geo_heatmap.py)
- [category_models.py](category_models.py)
- [vehicle_model.py](vehicle_model.py)
- [repost_detector.py](repost_detector.py)
- [listing_timeline.py](listing_timeline.py)
- [fresh_opportunity_detector.py](fresh_opportunity_detector.py)
- [market_density.py](market_density.py)
- [market_report.py](market_report.py)

**Novos templates (4):**
- [templates/geo_insights.html](templates/geo_insights.html)
- [templates/listing_timeline.html](templates/listing_timeline.html)
- [templates/fresh_deals.html](templates/fresh_deals.html)
- [templates/market_density.html](templates/market_density.html)

**Novos testes (6 arquivos, 53 testes):**
- test_geo_coverage, test_category_models, test_vehicle_model,
- test_repost_detector, test_fresh_opportunity, test_market_density

**Modificados:**
- [db.py](db.py) — 5 colunas novas + 2 tabelas + 5 índices via migração
- [monitor.py](monitor.py) — pipeline 12 → 16 etapas
- [web.py](web.py) — 4 rotas HTML + 1 API
- [templates/base.html](templates/base.html) — nav com 14 links

---

## 26. Sistema de watchers — monitoramento por palavra-chave e região (v8)

v8 entrega o **produto de monitoramento dirigido por usuário**: uma pessoa define um watcher como `(keyword="iphone", region="Araçatuba", min_price=..., max_price=...)`, o sistema faz backfill silencioso do que já existe indexado, e em passadas subsequentes alerta sobre listings novos que batem no filtro.

### 26.1 Realidade sobre "alerta imediato"

O pedido original era "alertar imediatamente quando um novo anúncio aparecer". **Isso não é viável com esta arquitetura** e foi documentado no topo do [watcher_engine.py](watcher_engine.py) e no alert amarelo em [templates/watchers.html](templates/watchers.html):

- Discovery usa DuckDuckGo via `discover_links.DuckDuckGoBackend`. O reindex do DDG para conteúdo novo do FB leva **minutos a horas**
- Rate limit conservador (~4s entre extracts, 5–9s entre páginas de DDG) é necessário para não cair em CAPTCHA/login wall
- `extract_item.extract()` é bloqueante e precisa ser espaçado

**Latência realista: dezenas de minutos até a detecção.** Alerta em segundos só é viável via API oficial do Meta Commerce (parceria formal). Preferi documentar honestamente a limitação a prometer o que não entrego.

O que este watcher engine **entrega de verdade**:
- Backfill confiável do que o DDG já conhece no momento da criação
- Monitoramento periódico com dedup determinístico (mesmo `(watch_id, listing_id)` nunca vira match duas vezes graças ao `UNIQUE` + `INSERT OR IGNORE`)
- Alerta disparado na próxima passada do pipeline após a detecção

### 26.2 Schema v8

Duas tabelas novas em [db.py](db.py):

```sql
CREATE TABLE watchers (
    watch_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      TEXT,              -- nullable, para multi-user futuro
    keyword      TEXT NOT NULL,
    region       TEXT,
    min_price    REAL,
    max_price    REAL,
    is_active    INTEGER NOT NULL DEFAULT 1,
    last_run_at  TEXT,
    created_at   TEXT NOT NULL
);

CREATE TABLE watcher_results (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    watch_id            INTEGER NOT NULL,
    listing_id          TEXT NOT NULL,
    first_seen          TEXT NOT NULL,
    is_initial_backfill INTEGER NOT NULL DEFAULT 0,
    UNIQUE(watch_id, listing_id),     -- ← dedup idempotente
    FOREIGN KEY (watch_id) REFERENCES watchers(watch_id)
);
```

O `UNIQUE(watch_id, listing_id)` é central: permite que o watcher rode quantas vezes quiser sem gerar matches duplicados. `INSERT OR IGNORE` no código explora isso.

### 26.3 `watcher_engine.py` — núcleo

**Função pura `matches_watcher(listing, watcher)`** — decide se um `Listing` extraído bate no filtro, retornando `(bool, reason)`. Separada do IO justamente para ser testável isoladamente. Faz:
- Keyword: substring case/accent-insensitive no título (usando `_strip_accents` + `_norm`)
- Region: substring case/accent-insensitive em `location_text`
- Price: respeita `min_price`/`max_price`, requer preço parseável no listing para aplicar o filtro (`parse_price` do price_normalizer)

14 testes cobrem todos os caminhos — match positivo/negativo por critério, combinações, edge cases (region com acento, sem preço no listing mas sem filtro de preço, etc.).

**`create_watch(keyword, region=None, min_price=None, max_price=None, user_id=None)`** — insere em `watchers`, retorna `watch_id`. Valida que keyword não é vazio/whitespace.

**`run_backfill(watch_id, max_pages=3)`** — descoberta inicial:
1. Constrói query DDG combinando `keyword + region`
2. Extrai cada hit com rate limit de 4s (`EXTRACT_DELAY_SECONDS`)
3. Filtra via `matches_watcher` (pulando listings já vistos neste watcher)
4. Grava em `watcher_results` com `is_initial_backfill=1`
5. **Não emite** evento `watcher_match` — backfill é silencioso por contrato

**`monitor_watch(watch_id, max_pages=2)`** — rodada subsequente:
- Mesma descoberta e extração
- Mas agora qualquer match que não estava em `watcher_results` para este `watch_id` vira:
  1. Linha nova em `watcher_results` com `is_initial_backfill=0`
  2. Evento `watcher_match` com `new_value="watch_id=N"` para rastreamento

**`run_due_watchers(min_interval_seconds=3600)`** — orquestrador para o monitor:
- Lê todos os `is_active=1`
- Seleciona aqueles com `last_run_at is None OR < now - interval`
- Chama `monitor_watch` em cada um em `try/except` isolado
- Agrega stats

Isso é o que o pipeline chama a cada passada. Default é 1h — evita martelar o DDG e manter rate limit saudável. Cada watcher decide quando é sua vez.

### 26.4 `alert_engine.py` — transporte de alertas de watcher

Reutiliza `send_telegram` e `send_discord` de [alerts.py](alerts.py) (v4), então env vars (`TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `DISCORD_WEBHOOK_URL`) são as mesmas.

**Diferença conceitual:** `alerts.py` dispara por score genérico (opportunity_score ≥ 80 ou discount ≥ 30). `alert_engine.py` dispara por **match de watcher** — independentemente de score, se o usuário pediu para monitorar "iphone em Araçatuba" e apareceu, alerta.

**Dedup por canal** em [_channel_dedup_key](alert_engine.py):
```
"watcher_telegram_{watch_id}"
"watcher_discord_{watch_id}"
```

Usa o mesmo `event_type='alert_sent'` existente no schema, com `old_value = dedup_key`. Reprocessar eventos `watcher_match` é idempotente — a dedup garante zero reenvio.

**`process_pending_watcher_matches(dry_run=False)`** — varre os últimos 200 eventos `watcher_match`, extrai `watch_id` do `new_value`, chama `send_for_match` para cada. Stats agregadas por canal + dedup + unconfigured.

**`format_watcher_alert(listing, watcher)`** — mensagem compacta:
```
🎯 Match no watcher #7
filtro: "iphone" em Araçatuba

iPhone 13 128GB preto seminovo
preço: 3500 BRL
local: Araçatuba, SP
https://www.facebook.com/marketplace/item/123/
```

### 26.5 Integração no pipeline do monitor

[monitor.py:_run_intelligence_pipeline](monitor.py) ganhou 2 estágios no final (agora **18 etapas**):

```
... (16 estágios anteriores)
17. watcher_engine.run_due_watchers       ← NOVO
18. alert_engine.process_pending_watcher_matches   ← NOVO
19. alerts.scan_and_alert                 (score-based, legado)
```

Watchers rodam **depois** do enriquecimento (market_value, score, etc.) porque o alerta inclui `opportunity_score` e `discount_percentage` do listing — queremos que essas colunas estejam fresquinhas. Watchers rodam **antes** dos alertas score-based para que o dashboard mostre os dois sinais (watcher_match e alert_sent) na mesma passada.

Cada um em `_safe()` — falha numa etapa loga ERROR mas não derruba o resto. Já era o padrão do pipeline.

Linha do console:
```
[pipeline] ... watchers=3 watcher_alerts=2 alerts=1
```

### 26.6 Dashboard `/watchers` — CRUD completo

**Rotas** em [web.py](web.py):

| Método | Rota | Ação |
|---|---|---|
| GET | `/watchers` | Lista + formulário de criação |
| POST | `/watchers` | Cria watcher (com opção de rodar backfill em background) |
| GET | `/watchers/{id}` | Detalhe: watcher + lista de matches (backfill vs. new) |
| POST | `/watchers/{id}/toggle` | Ativa/pausa |
| POST | `/watchers/{id}/backfill` | Rerroda backfill (background task) |
| POST | `/watchers/{id}/monitor` | Força monitor_watch agora (background task) |
| POST | `/watchers/{id}/delete` | Apaga watcher + todos os matches |

**BackgroundTasks do FastAPI** para `backfill` e `monitor` porque esses podem demorar 30s–2min (dependem de DDG + extract + rate limit) e travaria o HTTP. Background não bloqueia o redirect — o usuário volta para `/watchers` imediatamente e os dados aparecem na próxima atualização da página.

**Dependência nova:** `python-multipart` para `FastAPI.Form(...)`. Adicionada em [requirements.txt](requirements.txt) e instalada.

**Templates:**
- [templates/watchers.html](templates/watchers.html) — lista + form inline + banner amarelo explicando a latência honestamente
- [templates/watcher_detail.html](templates/watcher_detail.html) — metadados do watcher + ações + tabela de matches com linhas amarelas destacando os "NEW" (pós-backfill) vs cinzas do backfill inicial

Nav atualizada em [base.html](templates/base.html) com **Watchers** em amarelo/bold — é o produto principal.

### 26.7 Testes — 261 passando (29 novos v8)

| Arquivo | Testes | Cobertura |
|---|---|---|
| `test_watcher_engine.py` | 22 | matches_watcher por critério (10), create_watch + schema (5), run_due_watchers com mock (4), `_norm`/`_parse_iso` (3) |
| `test_alert_engine.py` | 7 | format_watcher_alert (2), process com env missing, dedup via alert_sent, dry_run, mocks de telegram/discord |

Os testes usam **DB isolado via `tmp_path` + `monkeypatch`** no fixture `isolated_db`, e fazem monkey-patch do `connect` dentro do módulo sob teste — padrão já consolidado nas versões anteriores do projeto.

Os testes de `run_due_watchers` usam `monkeypatch` do `monitor_watch` para **não bater no DDG real**, verificando apenas:
- Respeita intervalo (não roda se last_run recente)
- Roda se stale
- Pula inativos
- Sobrevive exceção em uma iteração (isola failures)

Total da suíte: **232 → 261 testes**, execução em ~3.9s.

### 26.8 Exemplo de uso end-to-end

```bash
# Via CLI
python watcher_engine.py create --keyword iphone --region Araçatuba --max-price 4000
#   → created watch_id=1
python watcher_engine.py backfill 1
#   → discovered 14, matched 6 (silencioso, sem alerta)
python watcher_engine.py monitor 1
#   → new_matches=0   (nada novo ainda)

# ... 2 horas depois, novo anúncio aparece e é indexado pelo DDG ...
python watcher_engine.py monitor 1
#   → new_matches=1   (emite watcher_match event)

python alert_engine.py process
#   → telegram_sent=1 discord_sent=1  (se env vars configuradas)

# Ou tudo automático via monitor
python monitor.py --from-db --interval 21600
#   → a cada 6h roda todos os 18 estágios, inclusive watchers + alertas
```

**Via dashboard:**
```bash
uvicorn web:app --port 8000
```
Abre `/watchers`, preenche formulário (keyword: `iphone`, region: `Araçatuba`, max: `4000`), marca "rodar backfill", clica Criar. Watcher aparece na lista com `last_run_at` preenchido em ~1min. Cada passada subsequente do monitor (via cron ou daemon) traz novos matches.

### 26.9 Não foram encontrados bugs reais pelos testes v8

Todos os 29 testes passaram de primeira. A função `matches_watcher` foi projetada pura desde o início e os casos de borda (accent insensitivity, None em price, region empty, etc.) foram cobertos no ato da escrita do teste — prova de que fazer parsing correto em `_norm` + `parse_price` desde módulos anteriores (v7 geo_coverage, v5 price_normalizer) paga dividendos aqui.

O único ajuste foi em runtime: `python-multipart` não estava instalado. FastAPI `Form(...)` exige essa lib. Adicionei em `requirements.txt` + instalei. Não é um bug do código, é dependência de produção.

---

## 27. Arquivos v8 — resumo

**Novos módulos (2):**
- [watcher_engine.py](watcher_engine.py)
- [alert_engine.py](alert_engine.py)

**Novos templates (2):**
- [templates/watchers.html](templates/watchers.html)
- [templates/watcher_detail.html](templates/watcher_detail.html)

**Novos testes (2 arquivos, 29 testes):**
- [tests/test_watcher_engine.py](tests/test_watcher_engine.py)
- [tests/test_alert_engine.py](tests/test_alert_engine.py)

**Modificados:**
- [db.py](db.py) — 2 tabelas novas (`watchers`, `watcher_results`) via migração idempotente
- [monitor.py](monitor.py) — pipeline 16 → 18 etapas (watcher_engine + alert_engine no final)
- [web.py](web.py) — 7 rotas novas (`/watchers` CRUD completo + detail + background tasks)
- [templates/base.html](templates/base.html) — link "Watchers" destacado em amarelo na nav
- [requirements.txt](requirements.txt) — `python-multipart>=0.0.9` para `Form(...)`

---

## 28. Discovery escalável e watchers paralelos (v9)

A v9 ataca dois gargalos do v8:
1. **Cobertura baixa** — um único query DDG perde muitos anúncios; agora rodamos N variações por watcher
2. **Throughput de watchers serial** — um watcher por vez não escala; agora rodamos N watchers em paralelo

Sem prometer "alerta em segundos" (ainda limitado pela latência de indexação do DDG, ver seção 26.1), v9 reduz o intervalo efetivo para watchers de alta prioridade e aumenta substancialmente o número de listings descobertos por keyword.

### 28.1 Mudanças de schema

Migração idempotente em [db.py:_migrate_columns](db.py) adiciona:

| Item | Tipo | Quem popula |
|---|---|---|
| `watchers.priority` | INTEGER NOT NULL DEFAULT 2 | usuário no `create_watch()` |
| `listings.very_recent_listing` | INTEGER NOT NULL DEFAULT 0 | `recent_listing_detector.detect()` |
| Tabela `discovery_cache` | — | `discovery_cache.put()` |

Dois índices novos: `idx_watchers_priority` e `idx_dc_expires`.

### 28.2 `keyword_expander.py` — variações por keyword

Pure rule-based (sem ML). Estratégia em 4 camadas, ordenadas por prioridade:

1. **Versões / modelos conhecidos** (`VERSION_EXPANSIONS`): `iphone` → `iphone 11`, `iphone 12`, `iphone 13`, `iphone 14`, `iphone 15`, `iphone xr`, `iphone xs`, `iphone se`. Match exato no trigger ou prefix com espaço — evita que `"iphone case"` gere as expansões de versão.
2. **Marca explícita** (`BRAND_EXPANSIONS`): `iphone` → `iphone apple`. Adiciona contexto que o usuário muitas vezes omite.
3. **Sinônimos diretos** (`SYNONYMS`): `notebook` ↔ `laptop`, `celular` ↔ `smartphone`. Bidirecional.
4. **Modificadores genéricos** (`COMMON_MODIFIERS`): sempre anexa "usado", "seminovo", "novo", "barato".

Listas curadas no topo do módulo. Estender = adicionar entrada. Determinístico, idempotente, ordenação estável. **Por que não LLM:** o universo do Marketplace BR é pequeno e mutável, listas curadas são tão eficazes quanto e infinitamente mais rápidas/baratas/explicáveis.

12 testes em [test_keyword_expander.py](tests/test_keyword_expander.py) cobrindo cada camada + edge cases (vazio, max_variations, no duplicates, normalização de whitespace).

### 28.3 `discovery_cache.py` — TTL via SQLite

Tabela `discovery_cache(query_hash, query_text, region, result_json, expires_at, created_at)`. Hash via SHA-256 sobre `f"{query.lower().strip()}|{region.lower().strip()}"` truncado em 32 chars como PK.

API mínima:
```python
get(query, region) -> list[dict] | None    # None se miss/expirado
put(query, region, results, ttl_seconds=600)
cleanup_expired() -> int
invalidate(query=None, region=None) -> int
info() -> {total, live, expired}
```

**TTL default 10 min.** Curto o suficiente para não atrapalhar detecção de anúncios novos, longo o suficiente para deduplicar entre múltiplos watchers da mesma keyword na mesma janela.

11 testes em [test_discovery_cache.py](tests/test_discovery_cache.py) cobrindo roundtrip, normalização de chave, expiração, cleanup, invalidação seletiva, JSON corrompido (retorna None ao invés de crashar).

### 28.4 `marketplace_discovery_engine.py` — coordenador multi-estratégia

Para um `(keyword, region)`, executa cada variação de `keyword_expander.expand()` como uma query DDG separada, deduplica resultados por `item_id`, atribui `source_query` em cada hit (saber qual variação trouxe qual anúncio é útil para entender quais expansões pagam).

```python
discover_for(keyword, region=None, max_pages=2, max_variations=6, use_cache=True)
  → {variations_tried, queries_run, cache_hits, total_unique_hits, hits: [...]}
```

**Integração com cache:** cada variação consulta `discovery_cache.get()` antes de chamar DDG. Hit → reutiliza. Miss → DDG + `discovery_cache.put()`. Em uma corrida com 6 variações, se 4 estão em cache, fazemos só 2 calls DDG na verdade.

**Tratamento de erro:** queries individuais que falham são logadas e puladas — não quebram o lote inteiro. Discovery é best-effort por construção.

### 28.5 `recent_listing_detector.py` — flag binária `very_recent_listing`

Critérios para flag = 1:
- `first_seen_at` < `WINDOW_MINUTES` (default 60) atrás
- `count(price_history)` ≤ 1 (sem oscilação de preço)
- `count(events)` ≤ 3 (pouca história)

Reset: listings que estavam flagged mas estão fora da janela são limpos para 0 na mesma passada. **A flag é volátil por design** — só representa o estado AGORA. Para detecção persistente, use `event_type='fresh_opportunity'` que já existe.

Diferença vs. `fresh_opportunity_detector` (v7): `fresh_opportunity_detector` é score 0..100 focado em deal-quality. Este aqui é flag binária focada em "acabou de aparecer". Ambos coexistem.

8 testes em [test_recent_listing_detector.py](tests/test_recent_listing_detector.py) — função pura `is_very_recent()` em todos os critérios + integração contra DB seedado verificando flag/reset.

### 28.6 `watcher_optimizer.py` — prewarm de cache para grupos populares

Detecta grupos `(keyword, region)` com ≥2 watchers ativos (ajustável via `min_users`). Para cada grupo, roda `marketplace_discovery_engine.discover_for()` proativamente, populando o cache. Watchers individuais subsequentes hit no cache ao invés de fazer DDG redundante.

**Por que prewarm e não shared discovery direta:** cada watcher pode ter `min_price`/`max_price` diferentes mesmo compartilhando keyword/region. A discovery (DDG) é compartilhável; o filtro de preço é por watcher. O cache TTL é exatamente esse ponto de compartilhamento.

`find_popular_groups(min_users=2)` faz o **GROUP BY em Python**, não em SQL — porque o `LOWER()` do SQLite só funciona com ASCII. `LOWER('AÇAÍ')` retorna `'AÇAÍ'`, então grupos com cedilha caem em buckets diferentes via SQL puro. **Bug encontrado pelos testes (28.13).**

### 28.7 `discovery_stats.py` — cobertura agregada

Computado em runtime, sem persistência:
- `stats_by_keyword(top)` — top tokens por contagem de listings ativos (via `title_normalizer.tokens`)
- `stats_by_region(top)` — top cidades por count de listings
- `detection_rate(days)` — novos listings nos últimos N dias + breakdown por dia
- `cache_summary()` — total/live/expired do `discovery_cache`

`build_report()` agrega tudo em um `StatsReport` dataclass. Consumido pelo dashboard `/discovery-stats`.

### 28.8 `product_metrics.py` — funil descoberta → alerta

Métricas diárias dos últimos N dias:
- Novos listings por dia
- Watcher matches por dia
- Alerts enviados por dia
- Distribuição do delta `first_seen → alert_sent` (mediana, p25, p75, p90)

**Aviso honesto** documentado no docstring e na UI: o delta `first_seen → alert` é **tempo de processamento INTERNO**, não tempo desde a publicação real no FB. O verdadeiro tempo "publicação → alerta" tem um piso adicional dado pela latência de indexação do DDG, que não conseguimos medir. Documentar esta limitação é mais útil que esconder.

### 28.9 `watcher_engine.py` — priority + async paralelo

Mudanças em [watcher_engine.py](watcher_engine.py):

**1. `create_watch()` aceita `priority` (default 2):**
- Priority 1 → intervalo 10 min (`PRIORITY_INTERVALS[1] = 600`)
- Priority 2 → intervalo 30 min (default)
- Priority 3 → intervalo 1 h
- Validação: priorities fora de `{1, 2, 3}` levantam `ValueError`

**2. `_select_due_ids(rows, fallback_interval)`** — função auxiliar pura. Para cada watcher, calcula seu próprio intervalo via `interval_for_priority(r["priority"])` e checa se `last_run_at` está fora dele. Reusada por sync e async. Watchers de prioridade alta rodam mais frequente.

**3. `_discover_hits()` agora usa `marketplace_discovery_engine.discover_for()`** ao invés de `discover_links.discover()` direto. Ganho automático de variações + cache TTL para todos os watchers existentes.

**4. `run_due_watchers_async(concurrency=3)`** — versão paralela:
```python
sem = asyncio.Semaphore(concurrency)
async def _run_one(wid):
    async with sem:
        try:
            return await asyncio.to_thread(monitor_watch, wid)
        except Exception as e:
            return e  # sentinela, conta como failure

results = await asyncio.gather(*[_run_one(wid) for wid in due_ids])
```

**Por que `asyncio.to_thread` e não `extract_async`:** o `monitor_watch` chama `extract()` que ainda é sync via `requests`. Reescrever tudo para async seria enorme. `to_thread` libera o GIL durante o IO de rede, então N=3 watchers em threads dão paralelismo real para o IO-bound do extract. Mais simples, mesmo ganho prático.

**Isolamento de falhas:** `asyncio.gather` com sentinel exceptions — se um watcher quebra, os outros seguem. Logs separam ran/failures.

A versão sync `run_due_watchers()` continua existindo para a CLI (`python watcher_engine.py run-due`).

### 28.10 Pipeline do monitor — agora com paralelismo

[monitor.py](monitor.py):

A camada de watchers foi **extraída do `_run_intelligence_pipeline()` sync** e movida para `run_pass()` que já é async. Razão: precisamos `await run_due_watchers_async()` no contexto async sem hacks de `asyncio.run()` aninhado.

```python
async def run_pass(...):
    ...
    if scan_opportunities_after and not STATE.stop:
        _run_intelligence_pipeline()         # sync — 16 etapas
        # v9: watchers em paralelo, async
        wch_result = await run_due_watchers_async(concurrency=concurrency)
```

E o `_run_intelligence_pipeline` ganhou 2 estágios novos antes do bloco de alertas:
```
... (etapas 1-15, incluindo fresh_opportunity)
16. recent_listing_detector.detect    ← NOVO
17. watcher_optimizer.prewarm_groups  ← NOVO (prewarm cache antes dos watchers)
18. alert_engine.process_pending_watcher_matches
19. alerts.scan_and_alert
```

(Watchers em si rodam em `run_pass`, não dentro do pipeline sync. A linha de log do console mostra ambos.)

### 28.11 Dashboard — 2 rotas novas

| Rota | Template | Função |
|---|---|---|
| `/watcher-insights` | [watcher_insights.html](templates/watcher_insights.html) | Top watchers por matches, distribuição por priority, time_to_alert, grupos populares |
| `/discovery-stats` | [discovery_stats.html](templates/discovery_stats.html) | Top keywords/regiões + detection rate + cache summary + chart de novos por dia |

API novas:
- `GET /api/discovery_stats?top=20&days=7` — JSON do report

Nav atualizada em [base.html](templates/base.html) com **W.Insights** e **Discovery** após Watchers.

### 28.12 Testes — 306 passando (45 novos v9)

| Arquivo | Testes | Cobertura |
|---|---|---|
| `test_keyword_expander.py` | 12 | cada camada de variação + edge cases + normalização |
| `test_discovery_cache.py` | 11 | roundtrip, key normalização, TTL expiração, cleanup, invalidate, JSON corrompido |
| `test_recent_listing_detector.py` | 8 | função pura + integração com DB |
| `test_watcher_v9.py` | 14 | priority CRUD + interval_for_priority + _select_due_ids + run_due_watchers_async (basic, isolated failures, inactive, no due) + find_popular_groups (group, case insensitive Unicode, exclude inactive, threshold) |

Total: **261 → 306 testes**, ~7.2s.

### 28.13 Bug real encontrado pelos testes (v9)

**`find_popular_groups` quebrava com Unicode case-insensitive.** O teste `test_find_popular_groups_case_insensitive` cria dois watchers `iPhone/ARAÇATUBA` e `iphone/araçatuba`. SQLite `LOWER()` e `COLLATE NOCASE` só funcionam com ASCII, então `Ç`/`ç` viram dois grupos diferentes. Resultado: o GROUP BY em SQL retornava 0 grupos, não 1.

**Fix:** mover o GROUP BY de SQL para Python. `str.lower()` do Python normaliza Unicode corretamente. A query agora é só `SELECT watch_id, keyword, region FROM watchers WHERE is_active = 1` e o agrupamento é em Python via dict bucketization. Trade-off: leitura full table ao invés de aggregate em SQL — aceitável até 10k+ watchers, e `is_active` tem índice. Documentado no docstring da função.

Esse é o tipo de bug que só aparece em produção em ambientes BR — caçar com teste em pt-BR antes de merge é a forma certa de descobrir.

### 28.14 Performance baseline (DB vazio)

```
v8: ~140 ms
v9: ~151 ms   (+11ms)
```

Delta vem de 2 ALTER TABLE + 1 CREATE TABLE + 2 CREATE INDEX da migração. Após init, custo recorrente do pipeline expandido é:
- `recent_listing_detector` ~ O(N_recent) (só listings na janela)
- `watcher_optimizer.prewarm_groups` ~ O(grupos populares × DDG) — aproveita cache TTL
- `run_due_watchers_async` ~ O(due / concurrency × tempo_por_watcher) — antes era O(due × tempo)

Para um cenário de 6 watchers todos due, com tempo médio de 30s cada:
- v8 sequencial: ~180s
- v9 paralelo (concurrency=3): ~60s (3× speedup)

O speedup real depende de como o DDG/extract distribui o tempo de IO. Em prática, o ganho é entre 2× e 4×.

---

## 29. Execução — comandos v9

### Pipeline contínuo (16 stages sync + watchers async paralelos)
```bash
python monitor.py --from-db --interval 21600 --concurrency 3
```

### Comandos novos v9
```bash
# Keyword expander
python keyword_expander.py iphone
python keyword_expander.py "playstation 5" --max 12

# Discovery engine multi-strategy
python marketplace_discovery_engine.py iphone --region Araçatuba
python marketplace_discovery_engine.py "playstation 5" --max-variations 6 --json

# Cache de discovery
python discovery_cache.py info
python discovery_cache.py cleanup
python discovery_cache.py clear

# Recent listing detector
python recent_listing_detector.py
python recent_listing_detector.py --window 30 --dry-run

# Watcher optimizer (prewarm cache para grupos populares)
python watcher_optimizer.py
python watcher_optimizer.py --summary
python watcher_optimizer.py --min-users 3

# Discovery stats
python discovery_stats.py
python discovery_stats.py --top 30 --days 14 --json

# Product metrics
python product_metrics.py --days 14
python product_metrics.py --days 30 --json

# Watcher com priority
python watcher_engine.py create --keyword iphone --region Araçatuba \
       --max-price 4000   # priority 2 default — para CLI usar API direta:
# (CLI não expõe --priority ainda; via dashboard ou Python:
#  from watcher_engine import create_watch
#  create_watch("iphone", "Araçatuba", priority=1, max_price=4000)
# )
```

### Dashboard — 2 rotas novas
```bash
uvicorn web:app --port 8000

#   /watcher-insights    top watchers, priority breakdown, time-to-alert, popular groups
#   /discovery-stats     keywords/regiões/detection rate/cache + chart de novos/dia
#   /api/discovery_stats JSON
```

### Tests
```bash
pytest tests/                           # 306 passing
pytest tests/test_watcher_v9.py -v
pytest tests/test_keyword_expander.py -v
```

---

## 30. Arquivos v9 — resumo

**Novos módulos (8):**
- [keyword_expander.py](keyword_expander.py)
- [discovery_cache.py](discovery_cache.py)
- [marketplace_discovery_engine.py](marketplace_discovery_engine.py)
- [recent_listing_detector.py](recent_listing_detector.py)
- [watcher_optimizer.py](watcher_optimizer.py)
- [discovery_stats.py](discovery_stats.py)
- [product_metrics.py](product_metrics.py)
- (+ atualizações em watcher_engine.py)

**Novos templates (2):**
- [templates/watcher_insights.html](templates/watcher_insights.html)
- [templates/discovery_stats.html](templates/discovery_stats.html)

**Novos testes (4 arquivos, 45 testes):**
- test_keyword_expander, test_discovery_cache, test_recent_listing_detector, test_watcher_v9

**Modificados:**
- [db.py](db.py) — `watchers.priority`, `listings.very_recent_listing`, tabela `discovery_cache`
- [watcher_engine.py](watcher_engine.py) — priority CRUD, `_discover_hits` usa o engine, `_select_due_ids` por priority, `run_due_watchers_async` paralelo via `asyncio.to_thread + Semaphore`
- [monitor.py](monitor.py) — extrai watchers da pipeline sync; `await run_due_watchers_async` em `run_pass`; adiciona `recent_listing_detector` e `watcher_optimizer.prewarm_groups` ao pipeline sync
- [web.py](web.py) — rotas `/watcher-insights`, `/discovery-stats`, `/api/discovery_stats`
- [templates/base.html](templates/base.html) — links W.Insights e Discovery

---

## 31. BuscaPlace v10 — Discovery profundo, prioridade e SaaS-readiness

A v10 ataca três coisas em paralelo:
1. **Cobertura**: deep discovery via BFS sobre `discovery_graph` — cada anúncio descoberto vira fonte de novas queries
2. **Latência**: alert priority + watcher scheduler + continuous_watchers — usuários pagantes/queries populares têm precedência
3. **Escala**: distributed_discovery worker pool + scale_simulator validando até 1000+ watchers

### 31.1 Honestidade upfront sobre 3 ambiguidades do escopo

Antes de detalhar, deixei explícito no docstring de cada módulo:

1. **"Distribuído"** = paralelo via `asyncio.Queue` num único processo. NÃO usa Redis/Celery. O padrão (producer/queue/workers) traduz limpo para multi-process quando trocar a queue por uma externa. Trade-off: trazer Celery+Redis adicionaria 2 deps pesadas + um broker pra gerenciar — pra escala atual, asyncio basta. O dia de migrar, [distributed_discovery.py](distributed_discovery.py) é o ponto único de troca.

2. **"Contínuo"** ([continuous_watchers.py](continuous_watchers.py)) NÃO é zero-latência. Tick mínimo prático é dezenas de segundos porque DDG limita rate. Ganho vs `monitor.py`: tempo de reação ~minutos ao invés de horas, não ~ms.

3. **`opportunity_probability`** ([opportunity_predictor.py](opportunity_predictor.py)) NÃO é modelo treinado. É heurística calibrada. Sem ground truth confiável de venda real, não dá pra treinar supervisado. Pesos manuais documentados; podem ser otimizados via score_optimizer-like analysis quando houver dados de outcome reais.

### 31.2 Schema v10

Migração idempotente em [db.py:_migrate_columns](db.py):

| Item | Tipo | Quem popula |
|---|---|---|
| `watchers.plan` | TEXT | usuário (`free`/`pro`/`premium`) |
| `listings.opportunity_probability` | REAL | `opportunity_predictor.predict_all()` |
| Tabela `discovery_graph` | — | `marketplace_deep_discovery.add_edge()` |

Tabela `discovery_graph`:
```sql
CREATE TABLE discovery_graph (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_query      TEXT,        -- '' para roots; veja 31.13 sobre o bug NULL UNIQUE
    child_query       TEXT NOT NULL,
    source_listing_id TEXT,
    depth             INTEGER NOT NULL DEFAULT 0,
    created_at        TEXT NOT NULL,
    UNIQUE(parent_query, child_query)
);
```

### 31.3 `related_listing_finder.py` — derivar queries de listings

Pure: dado um título, gera N queries derivadas via cascata em [derive_queries](related_listing_finder.py):
1. **brand + model** (mais preciso) — `Toyota Hilux SRV 2020 Diesel` → `toyota hilux`
2. **só model** — `hilux`
3. **só brand** — `toyota`
4. **brand + primeiro token significativo** (excluindo ano/marca/modelo)
5. **par de tokens significativos** (não-numéricos, ≥3 chars)

Filtros: dedup, queries muito curtas, queries só com stopwords. Anos (4 dígitos) nunca aparecem sozinhos.

9 testes em [test_related_listing_finder.py](tests/test_related_listing_finder.py).

### 31.4 `marketplace_deep_discovery.py` — BFS sobre discovery_graph

Estende `marketplace_discovery_engine` com expansão recursiva:
```
depth 0: query seed
depth 1: queries derivadas dos listings encontrados em depth 0
...
```

Termina por `max_depth` (default 2) OU `max_total_queries` (default 30). Cada query passa pelo discovery_cache TTL. Cada listing é dedupado por `item_id` globalmente. Um `visited_queries: set` impede recursão em loop.

Helpers do graph: `add_edge` (idempotente via INSERT OR IGNORE), `graph_summary`, `edges_from`, `all_known_queries`.

7 testes em [test_deep_discovery.py](tests/test_deep_discovery.py) — graph helpers + `deep_discover_for` end-to-end com `discover_for` mockado.

### 31.5 `opportunity_predictor.py` — probabilidade calibrada

Combinação linear de 5 sinais:
```
f1 = clamp(discount/50, 0..1)              * 0.35   ← desconto
f2 = liquidity_score / 100                  * 0.20   ← liquidez
f3 = max(token_velocity[t] for t in title)  * 0.20   ← velocidade do token
f4 = 1 if fraud<50 else 0                   * 0.15   ← ausência de fraude
f5 = 1 if not outlier else 0                * 0.10   ← preço dentro do esperado
probability = sum(f_i * w_i)  ∈ [0, 1]
```

`token_velocity_index` derivado de `market_density`: `1.0` para tokens com mediana ≤ 7 dias até venda, decai linearmente até 0 em 30 dias.

Persistido em `listings.opportunity_probability`. Exposto no dashboard `/top-opportunities`.

8 testes em [test_opportunity_predictor.py](tests/test_opportunity_predictor.py).

### 31.6 `alert_priority_engine.py` — re-ordenação de pendentes

Substitui `alert_engine.process_pending_watcher_matches` (v8) por uma versão ranqueada. Em volumes altos, watchers premium/pro não esperam atrás de free.

[compute_priority_score](alert_priority_engine.py):
```
base = opportunity_probability * 50    # 0..50
     + min(30, discount * 0.5)         # 0..30
     + min(20, fresh_score * 0.2)      # 0..20
     + min(10, liquidity / 10)         # 0..10

plan_boost: premium=1.5  pro=1.2  free/None=1.0
score = base * plan_boost              # max ~165
```

`process_with_priority()` busca os mesmos eventos `watcher_match`, **ranqueia primeiro**, e chama `alert_engine.send_for_match` em ordem desc. Dedup continua via `alert_sent` events.

10 testes em [test_alert_priority_engine.py](tests/test_alert_priority_engine.py).

### 31.7 `watcher_scheduler.py` — dynamic_priority dentro do batch

Ordena watchers DUE por dynamic_priority. v9 já tinha priority static; v10 refina:
```
base = 100 - priority*20      # P1=80, P2=60, P3=40
+ min(20, num_users*5)         # popularidade do (kw, region) compartilhado
+ min(20, match_count*0.5)     # histórico de matches
+ plan_bonus                   # premium=30, pro=15, free=0
```

Casos resolvidos:
- Watcher P2 premium (95) > watcher P1 free (85) — quem paga roda primeiro
- Watcher P2 popular com 5 users (80) > watcher P2 isolado (65)

`schedule_due()` retorna `list[int]` ordenada, restringindo aos DUE segundo intervalos individuais. `_load_watchers_with_context()` faz **GROUP BY popularity em Python** (não em SQL) — mesmo motivo do bug Unicode v9.

12 testes em [test_watcher_scheduler.py](tests/test_watcher_scheduler.py) — pure compute (8) + integração com DB seedado (4).

### 31.8 `distributed_discovery.py` — worker pool assíncrono

Padrão producer/queue/workers via `asyncio.Queue` + `asyncio.to_thread`:
```python
pool = DiscoveryWorkerPool(concurrency=5)
result = await pool.run([
    DiscoveryTask("iphone", "Araçatuba"),
    DiscoveryTask("playstation 5", "São Paulo"),
])
```

Trade-off documentado: para multi-process/multi-machine real, troque `asyncio.Queue` por Redis/RabbitMQ. A interface `DiscoveryWorkerPool.run()` é o ponto único de troca.

### 31.9 `continuous_watchers.py` — daemon loop

Diferente do `monitor.py` (pipeline COMPLETO periodicamente), este roda APENAS watchers em loop apertado:

```bash
python continuous_watchers.py --concurrency 5 --tick 30
```

Cada tick:
1. `run_due_watchers_async(concurrency)` — processa watchers due ordenados pelo scheduler
2. Se houve matches, `alert_priority_engine.process_with_priority()` envia em prioridade
3. Sleep até próximo tick (graceful shutdown via SIGINT)

Use case: rodar `continuous_watchers` como systemd service + `monitor.py` em cron diário (intelligence pesado). Watchers reagem mais rápido sem precisar do pipeline completo.

### 31.10 `scale_simulator.py` — benchmark interno

Mede overhead INTERNO do scheduler/queue/dispatch. NÃO mede DDG/extract reais — esses são mockados com `time.sleep` determinístico.

Cleanup automático via prefixo `__sim_`.

**Sweep medido em DB local:**

```
watchers=   50  conc=  5  delay=50ms  elapsed=  0.53s  thrput=  95.2/s  speedup=  4.8×
watchers=  100  conc=  5  delay=50ms  elapsed=  1.04s  thrput=  96.2/s  speedup=  4.8×
watchers=  100  conc= 10  delay=50ms  elapsed=  0.54s  thrput= 184.6/s  speedup=  9.2×
watchers=  500  conc= 10  delay=50ms  elapsed=  2.60s  thrput= 192.6/s  speedup=  9.6×
watchers= 1000  conc= 10  delay=50ms  elapsed=  5.17s  thrput= 193.3/s  speedup=  9.7×
watchers= 1000  conc= 20  delay=50ms  elapsed=  2.62s  thrput= 381.4/s  speedup= 19.1×
```

**Leitura honesta:**
- Speedup quase linear em concurrency (10× com c=10, 19× com c=20)
- Throughput estável ~190–380 watchers/s no nosso código
- 1000 watchers em 2.6s com c=20 → scheduler não é gargalo
- DDG real seria muito mais lento (5–10s/query); o limite real é o rate do DDG, não nosso código
- 1000 watchers reais com DDG: ~180 minutos serial vs ~9 minutos com c=20

### 31.11 Pipeline atualizado do monitor

[monitor.py:_run_intelligence_pipeline](monitor.py):
```
... (16 estágios v9) ...
17. recent_listing_detector
18. watcher_optimizer.prewarm_groups
19. opportunity_predictor.predict_all       ← NOVO v10
20. alert_priority_engine.process_with_priority   ← SUBSTITUI process_pending v8
21. alerts.scan_and_alert
```

Watchers continuam em paralelo via `await run_due_watchers_async()` em `run_pass()` (assíncrono), agora usando `watcher_scheduler.schedule_due()` por default para ordenar dentro do batch.

### 31.12 Dashboard — 3 rotas novas

| Rota | Template | Função |
|---|---|---|
| `/top-opportunities` | [top_opportunities.html](templates/top_opportunities.html) | listings ordenados por `opportunity_probability` desc com badge colorida |
| `/discovery-network` | [discovery_network.html](templates/discovery_network.html) | árvores BFS do `discovery_graph`, raízes + filhos depth=1 |
| `/watchers-performance` | [watchers_performance.html](templates/watchers_performance.html) | ranking por `dynamic_score`, mostra plan/popularidade/matches |

Nav atualizada em [base.html](templates/base.html) com **★Opps** (verde destacado), **Network**, **W.Perf**.

### 31.13 Bug real caçado pelos testes (v10)

**`add_edge` dedup falha quando `parent_query` é NULL.** SQLite UNIQUE constraints tratam NULL como sempre distinto de NULL — `(NULL, 'iphone')` aceita múltiplas linhas. Resultado: queries seed (depth=0, parent=NULL) duplicavam.

**Fix:** normalizar `parent_query=None → ''` no storage. Empty string é valor concreto e o UNIQUE funciona. Atualizei `graph_summary` e `edges_from` para tratar `WHERE parent_query IS NULL OR parent_query = ''` (defensivo — tolera dados legacy). Documentado no docstring de `add_edge`.

Ponto adicional: `test_run_due_watchers_async_basic` (do v9) quebrou porque a fixture não patcheava `watcher_scheduler.connect`, e o default `use_scheduler=True` em v10 passou a chamá-lo. Não é bug de produção, mas mostra que mudanças de default afetam testes downstream — atualizei a fixture para patchear watcher_scheduler também.

### 31.14 Testes — 352 passando (+46 v10)

| Arquivo | Testes | Cobertura |
|---|---|---|
| `test_related_listing_finder.py` | 9 | cascata brand+model, fallback, dedup, anos excluídos |
| `test_opportunity_predictor.py` | 8 | cada sinal isolado, fraud/outlier zerando, combinação máxima |
| `test_alert_priority_engine.py` | 10 | cada componente do score, plan boosts, watcher None |
| `test_watcher_scheduler.py` | 12 | compute_dynamic_priority puro (8) + integração DB (4) |
| `test_deep_discovery.py` | 7 | add_edge insert/dedup/multi-parent, summary, edges_from, BFS mockado |

Total: **306 → 352 testes**, ~9.4s.

---

## 32. Execução — comandos v10

```bash
# Pipeline completo (com opportunity_predictor + alert_priority + watchers paralelos)
python monitor.py --from-db --interval 21600 --concurrency 5

# Daemon contínuo (apenas watchers, tick apertado)
python continuous_watchers.py --concurrency 5 --tick 30

# Discovery profundo
python marketplace_deep_discovery.py iphone --region Araçatuba --max-depth 2
python marketplace_deep_discovery.py graph                    # resumo do grafo

# Worker pool distribuído
python distributed_discovery.py iphone "moto g" "playstation 5" --concurrency 5

# Predição e alertas priorizados
python opportunity_predictor.py
python alert_priority_engine.py --rank-only      # só lista ranking
python alert_priority_engine.py                  # processa e envia

# Scheduler debug
python watcher_scheduler.py
python related_listing_finder.py "iPhone 12 128GB preto"

# Simulação de escala
python scale_simulator.py --watchers 1000 --concurrency 20
python scale_simulator.py --sweep                # bateria completa

# Dashboard — 3 rotas novas
uvicorn web:app --port 8000
#   /top-opportunities    listings ordenados por opportunity_probability
#   /discovery-network    árvore BFS do discovery_graph
#   /watchers-performance ranking por dynamic_score

# Tests
pytest tests/                     # 352 passing
```

---

## 33. Arquivos v10 — resumo

**Novos módulos (8):**
- [related_listing_finder.py](related_listing_finder.py)
- [opportunity_predictor.py](opportunity_predictor.py)
- [alert_priority_engine.py](alert_priority_engine.py)
- [watcher_scheduler.py](watcher_scheduler.py)
- [marketplace_deep_discovery.py](marketplace_deep_discovery.py)
- [distributed_discovery.py](distributed_discovery.py)
- [continuous_watchers.py](continuous_watchers.py)
- [scale_simulator.py](scale_simulator.py)

**Novos templates (3):**
- [templates/top_opportunities.html](templates/top_opportunities.html)
- [templates/discovery_network.html](templates/discovery_network.html)
- [templates/watchers_performance.html](templates/watchers_performance.html)

**Novos testes (5 arquivos, 46 testes):**
- test_related_listing_finder, test_opportunity_predictor, test_alert_priority_engine, test_watcher_scheduler, test_deep_discovery

**Modificados:**
- [db.py](db.py) — `watchers.plan`, `listings.opportunity_probability`, tabela `discovery_graph`
- [watcher_engine.py](watcher_engine.py) — `create_watch(plan=...)`, `run_due_watchers_async(use_scheduler=True)` integra com scheduler
- [monitor.py](monitor.py) — pipeline ganha `opportunity_predictor` + `alert_priority_engine` no lugar de `process_pending_watcher_matches`
- [product_metrics.py](product_metrics.py) — `coverage_by_region` + `discovery_rate`
- [web.py](web.py) — rotas `/top-opportunities`, `/discovery-network`, `/watchers-performance`
- [templates/base.html](templates/base.html) — nav com 3 links (★Opps em verde)
- [tests/test_watcher_v9.py](tests/test_watcher_v9.py) — fixture patch'a watcher_scheduler.connect

---

## 34. Auditoria de código — achados e correções aplicadas

Revisão passada sobre todos os módulos novos. Achados:

1. **`discover_links.py` — precedência errada no display** — `h.title or '(sem título)'[:80]` aplicava o slice só ao literal, não ao título. Títulos longos apareciam inteiros no log. **Corrigido**: `(h.title or "(sem título)")[:80]`.
2. **`monitor.py` — `asyncio.get_event_loop()` deprecated** em Python 3.12+ dentro de coroutine. **Corrigido** para `asyncio.get_running_loop()` em `fetch_one()`.
3. **`monitor.py` — import não usado** (`events_for` de `db`). **Removido**.
4. **Conexão SQLite por listing em `reconcile()`** — abre e fecha uma conexão por item. Funcional mas ineficiente. **Decisão**: manter — torna o locking mais simples e o custo é irrelevante para volumes <10k/dia. Virar conexão por pass vira refactor não trivial porque `reconcile` precisa estar dentro de um único `with connect()`.
5. **`STATE` global em `monitor.py`** — singleton de processo. Aceitável para CLI; quebraria se `monitor.run_loop` fosse importado em múltiplos contextos. Trade-off consciente.
6. **Parser de JSON-LD vazio na prática** — o FB não expõe JSON-LD em Marketplace hoje, mas a camada fica pronta. Zero custo mantendo.
7. **`seller_name` frequentemente ausente** — comportamento esperado sem login. Não é bug.

## 35. Riscos residuais

- **Quebra do parser quando o FB mudar chaves Relay** — mitigado pelas camadas L1/L2 (estáveis) e pelo `field_sources`, que torna óbvio no dashboard qual campo caiu primeiro.
- **Scraping de DDG cair em anti-bot** — mitigado pelo delay 5–9s e por poder trocar o backend. Se acontecer, plugue SerpAPI.
- **Login wall permanente de um IP** — mitigado pela pausa global e pela concorrência baixa, mas não eliminado. Se acontecer, o único caminho correto é parar e reduzir taxa, não rotacionar proxy.
- **Dados pessoais (LGPD)** — mesmo em anúncio público, nome de vendedor e fotos de pessoas são dado pessoal. Não redistribua, não cruze com outras bases sem base legal, e armazene só o necessário.

Para uso pessoal / pesquisa delimitada com URLs conhecidas e rate baixo, este projeto está dentro do que considero defensável. Para uso comercial sério, o caminho correto é a API oficial de parceiros da Meta.
