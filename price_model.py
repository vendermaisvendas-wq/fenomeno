"""
Modelo de preço esperado (ML leve).

Dois backends, escolhidos em runtime:

  sklearn (preferido)
    - Features: HashingVectorizer(title) + brand_id + year + token_count
    - Target: log(price)
    - Modelo: GradientBoostingRegressor (n_estimators=80, default depth)
    - Treina em todos os listings ativos com preço > 0
    - `predict()` devolve exp(y_pred) — volta para escala original

  fallback (stdlib)
    - kNN sobre ComparablesIndex (já existente): predicted_price = mediana dos comparáveis
    - Isto é basicamente `estimated_market_value` do market_value.py, exposto
      aqui para dar API consistente quando sklearn não está disponível.

Cada listing recebe:
    predicted_price  REAL
    price_gap        REAL   (predicted - current)  — positivo = deal
    (atualizado em `listings`)

Uso:
    python price_model.py                    # treina e aplica a todos
    python price_model.py --backend fallback # força fallback
    python price_model.py --dry-run
"""

from __future__ import annotations

import argparse
import math
from dataclasses import dataclass

from db import connect, init_db
from logging_setup import get_logger, kv
from market_value import ComparablesIndex, _load_priced_items

log = get_logger("price_model")

MIN_TRAINING_SAMPLES = 30
SKLEARN_BACKEND = "sklearn"
FALLBACK_BACKEND = "fallback"


def _has_sklearn() -> bool:
    try:
        import sklearn  # noqa: F401
        return True
    except ImportError:
        return False


# --- feature engineering (sklearn backend) -----------------------------

def _build_feature_matrix(items: list, brand_index: dict[str, int]):
    """Retorna X (sparse/dense matrix) e y. Importação de sklearn localizada."""
    from sklearn.feature_extraction.text import HashingVectorizer
    import numpy as np
    from scipy.sparse import hstack, csr_matrix

    vec = HashingVectorizer(
        n_features=2**12, alternate_sign=False, norm=None,
        analyzer="word", lowercase=True,
    )
    titles = [it.title for it in items]
    X_text = vec.transform(titles)  # sparse

    # numeric features
    n = len(items)
    X_num = np.zeros((n, 3), dtype=np.float32)
    for i, it in enumerate(items):
        X_num[i, 0] = brand_index.get(it.brand or "_unk", 0)
        X_num[i, 1] = (it.year or 0)
        X_num[i, 2] = len(it.tokens)

    X = hstack([X_text, csr_matrix(X_num)])

    y = np.array([math.log1p(it.price) for it in items], dtype=np.float32)
    return X, y, vec


@dataclass
class ModelState:
    backend: str
    trained: bool
    n_samples: int
    note: str = ""


def _train_sklearn(items):
    from sklearn.ensemble import GradientBoostingRegressor

    brand_set = sorted({it.brand or "_unk" for it in items})
    brand_index = {b: i + 1 for i, b in enumerate(brand_set)}

    X, y, vec = _build_feature_matrix(items, brand_index)
    # GBR não aceita sparse direto → converter para dense se pequeno
    if X.shape[0] < 5000:
        X = X.toarray()

    model = GradientBoostingRegressor(
        n_estimators=80, max_depth=4, learning_rate=0.1, random_state=42
    )
    model.fit(X, y)
    return {"model": model, "vec": vec, "brand_index": brand_index}


def _predict_sklearn(state, items):
    from scipy.sparse import hstack, csr_matrix
    import numpy as np

    titles = [it.title for it in items]
    X_text = state["vec"].transform(titles)
    n = len(items)
    X_num = np.zeros((n, 3), dtype=np.float32)
    bi = state["brand_index"]
    for i, it in enumerate(items):
        X_num[i, 0] = bi.get(it.brand or "_unk", 0)
        X_num[i, 1] = it.year or 0
        X_num[i, 2] = len(it.tokens)
    X = hstack([X_text, csr_matrix(X_num)])
    if X.shape[0] < 5000:
        X = X.toarray()
    y_pred = state["model"].predict(X)
    return [math.expm1(y) for y in y_pred]


# --- fallback backend --------------------------------------------------

def _predict_fallback(items):
    """Fallback: para cada item, mediana do preço dos comparáveis. Idêntico
    ao estimated_market_value mas recalculado aqui para separação de concerns."""
    import statistics
    index = ComparablesIndex(items)
    preds = []
    for it in items:
        comps = index.find_comparables(it)
        if len(comps) < 3:
            preds.append(None)
            continue
        preds.append(statistics.median(c.price for c in comps))
    return preds


# --- orquestrador ------------------------------------------------------

def train_and_predict(backend: str = "auto", dry_run: bool = False) -> dict:
    init_db()
    with connect() as conn:
        items = _load_priced_items(conn, exclude_outliers=True)

    if len(items) < MIN_TRAINING_SAMPLES:
        log.warning(kv(
            event="insufficient_training_data",
            n=len(items), min=MIN_TRAINING_SAMPLES,
        ))
        return {
            "status": "insufficient_data",
            "n_samples": len(items),
            "backend": None,
        }

    # Escolha de backend
    if backend == "auto":
        backend = SKLEARN_BACKEND if _has_sklearn() else FALLBACK_BACKEND

    if backend == SKLEARN_BACKEND:
        if not _has_sklearn():
            log.warning(kv(event="sklearn_missing", fallback=True))
            backend = FALLBACK_BACKEND

    if backend == SKLEARN_BACKEND:
        state = _train_sklearn(items)
        predictions = _predict_sklearn(state, items)
    else:
        predictions = _predict_fallback(items)

    updated = 0
    updates: list[tuple[float, float, str]] = []
    for item, pred in zip(items, predictions):
        if pred is None or pred <= 0:
            continue
        gap = pred - item.price
        updates.append((float(pred), float(gap), item.id))
        updated += 1

    if not dry_run and updates:
        with connect() as conn:
            conn.executemany(
                "UPDATE listings SET predicted_price = ?, price_gap = ? WHERE id = ?",
                updates,
            )

    result = {
        "status": "ok",
        "backend": backend,
        "n_samples": len(items),
        "updated": updated,
    }
    log.info(kv(event="price_model_applied", **result))
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--backend", choices=["auto", SKLEARN_BACKEND, FALLBACK_BACKEND],
                    default="auto")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    result = train_and_predict(backend=args.backend, dry_run=args.dry_run)
    for k, v in result.items():
        print(f"  {k:15s} {v}")
    return 0 if result.get("status") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
