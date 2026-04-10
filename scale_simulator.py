"""
Simulador de escala — mede overhead INTERNO do scheduler/queue/dispatch.

⚠ NÃO MEDE: latência real de DDG ou de extract do FB. O ponto de medida
é o overhead interno do nosso código processando muitos watchers em paralelo.
DDG/FB são mockados com sleep determinístico.

Útil para responder: "se eu tiver 1000 watchers, nosso código aguenta?"
NÃO responde: "se eu tiver 1000 watchers, o DDG bloqueia?".

Cenários típicos:
    --watchers 100 --concurrency 5         small pilot
    --watchers 1000 --concurrency 10       sweet spot
    --watchers 5000 --concurrency 20       stress

Cleanup automático: cria watchers com prefixo `__sim_`, deleta no fim.

Uso:
    python scale_simulator.py --watchers 1000 --concurrency 10
    python scale_simulator.py --watchers 5000 --concurrency 20 --json
    python scale_simulator.py --sweep                    # roda múltiplos cenários
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import asdict, dataclass

from db import connect, init_db, now_iso
from logging_setup import get_logger

log = get_logger("scale_simulator")

SIM_KEYWORD_PREFIX = "__sim_"


@dataclass
class SimResult:
    num_watchers: int
    concurrency: int
    mock_delay_ms: int
    elapsed_seconds: float
    throughput_per_sec: float
    speedup_vs_serial: float
    ran: int
    failures: int


def _seed_synthetic_watchers(n: int) -> None:
    init_db()
    with connect() as conn:
        conn.execute(
            f"DELETE FROM watchers WHERE keyword LIKE '{SIM_KEYWORD_PREFIX}%'"
        )
        for i in range(n):
            conn.execute(
                """
                INSERT INTO watchers
                  (keyword, region, is_active, priority, created_at)
                VALUES (?, NULL, 1, 1, ?)
                """,
                (f"{SIM_KEYWORD_PREFIX}{i}", now_iso()),
            )


def _cleanup_synthetic_watchers() -> None:
    with connect() as conn:
        conn.execute(
            f"DELETE FROM watchers WHERE keyword LIKE '{SIM_KEYWORD_PREFIX}%'"
        )


async def simulate(
    num_watchers: int, concurrency: int, mock_delay_ms: int = 50,
) -> SimResult:
    _seed_synthetic_watchers(num_watchers)

    import watcher_engine as we
    original_monitor = we.monitor_watch

    def _sync_mock(watch_id: int) -> dict:
        # bloqueante intencional — simula extract sync.
        # to_thread libera o GIL durante o sleep, então paralelismo é real.
        time.sleep(mock_delay_ms / 1000)
        return {"new_matches": 0, "discovered": 0, "skipped": 0}

    we.monitor_watch = _sync_mock

    try:
        t0 = time.perf_counter()
        result = await we.run_due_watchers_async(concurrency=concurrency)
        elapsed = time.perf_counter() - t0
    finally:
        we.monitor_watch = original_monitor
        _cleanup_synthetic_watchers()

    serial_time = num_watchers * (mock_delay_ms / 1000)
    return SimResult(
        num_watchers=num_watchers,
        concurrency=concurrency,
        mock_delay_ms=mock_delay_ms,
        elapsed_seconds=round(elapsed, 3),
        throughput_per_sec=round(num_watchers / elapsed, 2) if elapsed > 0 else 0,
        speedup_vs_serial=round(serial_time / elapsed, 2) if elapsed > 0 else 0,
        ran=result.get("ran", 0),
        failures=result.get("failures", 0),
    )


async def sweep() -> list[SimResult]:
    """Roda alguns cenários comuns. Útil para baseline."""
    scenarios = [
        (50, 5), (100, 5), (100, 10),
        (500, 10), (1000, 10), (1000, 20),
    ]
    out = []
    for n, c in scenarios:
        print(f"[sweep] {n} watchers × concurrency {c} ...", flush=True)
        r = await simulate(n, c, mock_delay_ms=50)
        out.append(r)
    return out


def _print_one(r: SimResult) -> None:
    print(f"  watchers={r.num_watchers:>5d}  conc={r.concurrency:>3d}  "
          f"delay={r.mock_delay_ms}ms  "
          f"elapsed={r.elapsed_seconds:>7.2f}s  "
          f"thrput={r.throughput_per_sec:>7.1f}/s  "
          f"speedup={r.speedup_vs_serial:>5.1f}×")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--watchers", type=int, default=100)
    ap.add_argument("--concurrency", type=int, default=5)
    ap.add_argument("--mock-delay-ms", type=int, default=50)
    ap.add_argument("--sweep", action="store_true",
                    help="roda múltiplos cenários (ignora --watchers/--concurrency)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if args.sweep:
        results = asyncio.run(sweep())
        if args.json:
            print(json.dumps([asdict(r) for r in results], indent=2))
        else:
            print()
            for r in results:
                _print_one(r)
        return 0

    result = asyncio.run(simulate(
        args.watchers, args.concurrency, args.mock_delay_ms,
    ))

    if args.json:
        print(json.dumps(asdict(result), indent=2))
    else:
        print()
        _print_one(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
