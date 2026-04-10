"""
Extração estruturada de features de veículos a partir do título.

Objetivo: dar ao market_value um agrupamento de comparáveis *muito* mais
apertado para a categoria "vehicles", onde os detalhes (ano, combustível,
transmissão, cilindrada) importam muito no preço.

Exemplo:
    "Hilux SRV 2013 Diesel 4x4 Automatica"
    →
    VehicleFeatures(
        brand="toyota", model="hilux", year=2013,
        fuel="diesel", transmission="automatica", engine=None, traction="4x4",
    )

Depois, `find_vehicle_comparables(target, pool)` monta comparáveis por
cascata específica de veículos:
    1. mesma marca + mesmo modelo + ano ±1 + mesmo combustível
    2. mesma marca + mesmo modelo + ano ±2
    3. mesma marca + mesmo modelo (qualquer ano)
    4. mesma marca + mesmo combustível (modelos próximos)

Módulo é puro nas funções de extração — testável sem DB.

Uso:
    python vehicle_model.py extract "Hilux SRV 2013 Diesel 4x4"
    python vehicle_model.py apply                   # preenche estimated_market_value para vehicles
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
from dataclasses import asdict, dataclass

from db import connect, init_db
from logging_setup import get_logger, kv
from market_value import MIN_COMPARABLES, PricedItem, _load_priced_items
from title_normalizer import extract_brand, extract_year, tokens

log = get_logger("vehicle_model")

# Vocabulário estruturado específico de veículos
FUELS = {"diesel", "gasolina", "flex", "etanol", "gnv", "eletrico", "hibrido"}
TRANSMISSIONS = {"automatica", "automatico", "automatizada", "cvt",
                 "manual", "mecanica"}
TRACTIONS = {"4x4", "4x2", "awd", "fwd", "rwd"}

ENGINE_RE = re.compile(r"(?<!\d)(\d\.\d)(?!\d)")

# Modelos conhecidos (whitelist curta para ambiguidade como "civic" vs random tokens)
KNOWN_MODELS = {
    # Toyota
    "hilux", "corolla", "etios", "yaris", "rav4",
    # Honda
    "civic", "fit", "hrv", "crv", "cb", "cg", "biz", "titan", "pop",
    "xre", "cbr", "bros",
    # Chevrolet
    "onix", "cruze", "s10", "tracker", "prisma", "cobalt", "montana",
    # Ford
    "ka", "fiesta", "focus", "ranger", "ecosport", "fusion", "territory",
    # Fiat
    "uno", "palio", "strada", "argo", "cronos", "mobi", "toro", "punto",
    # VW
    "gol", "voyage", "polo", "virtus", "nivus", "saveiro", "tcross", "tcross",
    "jetta", "passat", "amarok",
    # Hyundai
    "hb20", "creta", "tucson", "santafe", "ix35",
    # Renault
    "kwid", "sandero", "logan", "duster", "captur", "oroch",
    # motos
    "factor", "fazer", "lander", "mt", "xtz", "crosser", "ybr",
    "twister", "hornet", "tornado",
}


@dataclass
class VehicleFeatures:
    brand: str | None
    model: str | None
    year: int | None
    fuel: str | None
    transmission: str | None
    engine: str | None        # "1.0", "2.0", etc
    traction: str | None      # "4x4", "4x2", ...


def extract(title: str | None) -> VehicleFeatures:
    if not title:
        return VehicleFeatures(None, None, None, None, None, None, None)

    toks = tokens(title)
    brand = extract_brand(title)
    year = extract_year(title)

    fuel = next((f for f in FUELS if f in toks), None)
    trans = next((t for t in TRANSMISSIONS if t in toks), None)
    traction = next((t for t in TRACTIONS if t in toks), None)

    engine_m = ENGINE_RE.search(title.lower())
    engine = engine_m.group(0) if engine_m else None

    # Modelo: primeiro hit em KNOWN_MODELS; senão primeiro token não-stopword
    # que não seja brand/year/fuel/transmission/traction/engine
    model = next(iter(sorted(toks & KNOWN_MODELS)), None)

    return VehicleFeatures(
        brand=brand, model=model, year=year,
        fuel=fuel, transmission=trans, engine=engine, traction=traction,
    )


# --- busca de comparáveis específica --------------------------------------

def _has_model_year_fuel(features: VehicleFeatures) -> bool:
    return bool(features.brand and features.model and features.year and features.fuel)


def find_vehicle_comparables(
    target: PricedItem, pool: list[PricedItem],
    features_cache: dict[str, VehicleFeatures] | None = None,
) -> list[PricedItem]:
    """Cascata específica de veículos (mais apertada que market_value genérico).

    Retorna lista de PricedItems que são "comparáveis" ao target.
    """
    if features_cache is None:
        features_cache = {}

    def feats(item: PricedItem) -> VehicleFeatures:
        if item.id not in features_cache:
            features_cache[item.id] = extract(item.title)
        return features_cache[item.id]

    tgt = feats(target)
    if tgt.brand is None or tgt.model is None:
        return []  # sem marca+modelo, não há como fazer match apertado

    def _brand_model_year_fuel(year_range: int) -> list[PricedItem]:
        out = []
        for p in pool:
            if p.id == target.id:
                continue
            pf = feats(p)
            if (pf.brand == tgt.brand
                    and pf.model == tgt.model
                    and pf.year is not None and tgt.year is not None
                    and abs(pf.year - tgt.year) <= year_range
                    and (tgt.fuel is None or pf.fuel == tgt.fuel)):
                out.append(p)
        return out

    def _brand_model_year(year_range: int) -> list[PricedItem]:
        out = []
        for p in pool:
            if p.id == target.id:
                continue
            pf = feats(p)
            if (pf.brand == tgt.brand
                    and pf.model == tgt.model
                    and pf.year is not None and tgt.year is not None
                    and abs(pf.year - tgt.year) <= year_range):
                out.append(p)
        return out

    def _brand_model() -> list[PricedItem]:
        return [
            p for p in pool
            if p.id != target.id
            and feats(p).brand == tgt.brand
            and feats(p).model == tgt.model
        ]

    # Cascata
    if tgt.year is not None and tgt.fuel is not None:
        c = _brand_model_year_fuel(year_range=1)
        if len(c) >= MIN_COMPARABLES:
            return c
        c = _brand_model_year_fuel(year_range=2)
        if len(c) >= MIN_COMPARABLES:
            return c

    if tgt.year is not None:
        c = _brand_model_year(year_range=2)
        if len(c) >= MIN_COMPARABLES:
            return c
        c = _brand_model_year(year_range=4)
        if len(c) >= MIN_COMPARABLES:
            return c

    # Fallback mais solto: só marca+modelo
    return _brand_model()


# --- batch apply ----------------------------------------------------------

def apply_vehicle_valuation(dry_run: bool = False) -> dict:
    """Para listings onde category='vehicles', recalcula estimated_market_value
    com cascata específica de veículos. Deixa o restante intocado."""
    init_db()
    with connect() as conn:
        vehicle_items = _load_priced_items(conn, exclude_outliers=True)
        # Precisamos filtrar apenas os de categoria vehicles
        vehicle_ids = {
            r["id"] for r in conn.execute(
                "SELECT id FROM listings WHERE category = 'vehicles'"
            ).fetchall()
        }
        vehicles = [it for it in vehicle_items if it.id in vehicle_ids]

    cache: dict[str, VehicleFeatures] = {}
    updates: list[tuple[float, float, str]] = []

    for target in vehicles:
        comps = find_vehicle_comparables(target, vehicles, cache)
        if len(comps) < MIN_COMPARABLES:
            continue
        prices = sorted(c.price for c in comps)
        median = statistics.median(prices)
        if median <= 0:
            continue
        discount = (median - target.price) / median * 100.0
        updates.append((median, discount, target.id))

    if not dry_run and updates:
        with connect() as conn:
            conn.executemany(
                "UPDATE listings SET estimated_market_value = ?, "
                "discount_percentage = ? WHERE id = ?",
                updates,
            )

    result = {
        "vehicles_in_pool": len(vehicles),
        "updated": len(updates),
    }
    log.info(kv(event="vehicle_valuation_applied", **result))
    return result


# --- CLI -------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    sp_ext = sub.add_parser("extract")
    sp_ext.add_argument("title")

    sp_app = sub.add_parser("apply")
    sp_app.add_argument("--dry-run", action="store_true")

    args = ap.parse_args()

    if args.cmd == "extract":
        feats = extract(args.title)
        print(json.dumps(asdict(feats), ensure_ascii=False, indent=2))
        return 0

    if args.cmd == "apply":
        result = apply_vehicle_valuation(dry_run=args.dry_run)
        for k, v in result.items():
            print(f"  {k:25s} {v}")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
