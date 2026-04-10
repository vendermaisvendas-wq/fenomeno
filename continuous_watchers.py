"""
Modo daemon contínuo dos watchers.

Diferença vs. monitor.py:
    monitor.py        → pipeline COMPLETO (intelligence + watchers) periodicamente
    continuous_watchers → APENAS watchers, em loop apertado, sempre rodando

Útil quando você quer rodar o intelligence pipeline em cron diário (caro)
e os watchers respondem mais rápido em background. É a forma de aproximar
"alerta rápido" sem trazer um broker externo.

Características:
    - asyncio loop infinito até SIGINT
    - cada tick chama watcher_engine.run_due_watchers_async com concurrency
    - usa watcher_scheduler para ordem prioritária dentro do batch
    - dorme `tick_seconds` quando não há nada due (default 30s)
    - chama alert_priority_engine ao final de cada tick para drenar alertas

⚠ HONESTIDADE SOBRE LATÊNCIA: tick mínimo prático é dezenas de segundos.
O DDG limita rate; "loop apertado" não significa "polling em ms". O ganho
real vs. monitor.py é tempo de reação ~minutos ao invés de horas.

Uso:
    python continuous_watchers.py
    python continuous_watchers.py --concurrency 5 --tick 30
    python continuous_watchers.py --no-alerts          # só descobre, não envia
"""

from __future__ import annotations

import argparse
import asyncio
import signal

from db import init_db
from logging_setup import configure as configure_logging, get_logger, kv

log = get_logger("continuous_watchers")


class State:
    stop = False


def _install_signal_handlers() -> None:
    def handler(signum, frame):  # noqa: ARG001
        State.stop = True
        print("\n[continuous] shutdown requested — terminando após o tick atual",
              flush=True)
    try:
        signal.signal(signal.SIGINT, handler)
    except (ValueError, AttributeError):
        pass


async def _tick(concurrency: int, send_alerts: bool) -> dict:
    from watcher_engine import run_due_watchers_async
    result = await run_due_watchers_async(concurrency=concurrency)

    if send_alerts and result.get("total_new_matches", 0) > 0:
        from alert_priority_engine import process_with_priority
        try:
            alerts_result = await asyncio.to_thread(process_with_priority)
            result["alerts"] = alerts_result
        except Exception as e:  # noqa: BLE001
            log.error(kv(event="alert_priority_failed",
                         error=type(e).__name__))

    return result


async def loop(concurrency: int, tick_seconds: int, send_alerts: bool) -> None:
    init_db()
    log.info(kv(event="continuous_loop_start",
                concurrency=concurrency, tick=tick_seconds))
    print(f"[continuous] starting (concurrency={concurrency}, "
          f"tick={tick_seconds}s, alerts={send_alerts})", flush=True)

    while not State.stop:
        try:
            result = await _tick(concurrency, send_alerts)
            ran = result.get("ran", 0)
            new = result.get("total_new_matches", 0)
            failures = result.get("failures", 0)
            if ran > 0 or new > 0:
                print(
                    f"[tick] ran={ran} new_matches={new} failures={failures}",
                    flush=True,
                )
        except Exception as e:  # noqa: BLE001
            log.error(kv(event="tick_failed", error=repr(e)[:200]))

        # Sleep com checagem de stop a cada segundo (graceful shutdown)
        for _ in range(tick_seconds):
            if State.stop:
                break
            await asyncio.sleep(1)

    log.info(kv(event="continuous_loop_stopped"))
    print("[continuous] stopped", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--concurrency", type=int, default=3)
    ap.add_argument("--tick", type=int, default=30,
                    help="segundos entre verificações de due (default 30)")
    ap.add_argument("--no-alerts", action="store_true",
                    help="não envia alerts (só descobre)")
    args = ap.parse_args()

    configure_logging()
    _install_signal_handlers()
    asyncio.run(loop(
        concurrency=args.concurrency,
        tick_seconds=args.tick,
        send_alerts=not args.no_alerts,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
