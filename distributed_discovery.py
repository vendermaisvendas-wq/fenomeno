"""
Worker pool assíncrono para discovery em paralelo.

⚠ DESAMBIGUIDADE HONESTA: "distribuído" aqui significa "paralelo via
asyncio dentro de um processo só". Não usa Redis/Celery/RabbitMQ. O padrão
é: producer enfileira tarefas → N workers consomem em paralelo. Esta
estrutura É a base que se traduz para multi-process/multi-machine quando
você trocar `asyncio.Queue` por uma fila externa — a API consumidora é a
mesma.

Trade-off explícito: trazer Celery/Redis adicionaria 2 deps pesadas e um
broker para gerenciar. Para o tamanho atual do projeto, asyncio.Queue
basta. O dia que precisar escalar entre máquinas, este arquivo é o ponto
de troca.

Padrão:
    pool = DiscoveryWorkerPool(concurrency=5)
    result = await pool.run(tasks=[
        DiscoveryTask("iphone", "Araçatuba"),
        DiscoveryTask("playstation 5", "São Paulo"),
        ...
    ])

Uso CLI:
    python distributed_discovery.py iphone "moto g" "playstation 5"
    python distributed_discovery.py --concurrency 10 iphone ipad macbook
"""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass, field

from logging_setup import get_logger, kv

log = get_logger("distributed_discovery")


@dataclass
class DiscoveryTask:
    query: str
    region: str | None = None
    max_pages: int = 2


@dataclass
class DiscoveryWorkerPool:
    concurrency: int = 5
    use_cache: bool = True
    _stats: dict = field(default_factory=lambda: {
        "tasks_total": 0,
        "tasks_done": 0,
        "tasks_failed": 0,
        "queries_run_ddg": 0,
        "cache_hits": 0,
    })

    async def run(self, tasks: list[DiscoveryTask]) -> dict:
        """Distribui `tasks` entre workers. Devolve agregado com hits dedupados."""
        if not tasks:
            return {**self._stats, "unique_hits": 0, "hits": []}

        queue: asyncio.Queue[DiscoveryTask] = asyncio.Queue()
        for t in tasks:
            queue.put_nowait(t)
        self._stats["tasks_total"] = len(tasks)

        seen_ids: set[str] = set()
        all_hits: list[dict] = []
        lock = asyncio.Lock()

        async def worker(name: str) -> None:
            while True:
                try:
                    task = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    result = await asyncio.to_thread(self._discover_one, task)
                    self._stats["queries_run_ddg"] += result.get("queries_run", 0)
                    self._stats["cache_hits"] += result.get("cache_hits", 0)
                    async with lock:
                        for h in result.get("hits", []):
                            iid = h.get("item_id")
                            if iid and iid not in seen_ids:
                                seen_ids.add(iid)
                                all_hits.append(h)
                    self._stats["tasks_done"] += 1
                    log.info(kv(worker=name, task=task.query,
                                hits=result.get("total_unique_hits", 0)))
                except Exception as e:  # noqa: BLE001
                    self._stats["tasks_failed"] += 1
                    log.error(kv(worker=name, task=task.query,
                                 error=type(e).__name__))
                finally:
                    queue.task_done()

        workers = [
            asyncio.create_task(worker(f"w{i}"))
            for i in range(self.concurrency)
        ]
        await asyncio.gather(*workers)

        return {**self._stats, "unique_hits": len(all_hits), "hits": all_hits}

    def _discover_one(self, task: DiscoveryTask) -> dict:
        from marketplace_discovery_engine import discover_for
        return discover_for(
            keyword=task.query,
            region=task.region,
            max_pages=task.max_pages,
            use_cache=self.use_cache,
        )


async def run_pool(
    queries: list[str], region: str | None = None, concurrency: int = 5,
) -> dict:
    """Helper alto nível: lista de queries → DiscoveryTasks → pool.run()."""
    tasks = [DiscoveryTask(query=q, region=region) for q in queries]
    pool = DiscoveryWorkerPool(concurrency=concurrency)
    return await pool.run(tasks)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("queries", nargs="+")
    ap.add_argument("--region")
    ap.add_argument("--concurrency", type=int, default=5)
    ap.add_argument("--no-cache", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    pool = DiscoveryWorkerPool(
        concurrency=args.concurrency, use_cache=not args.no_cache,
    )
    tasks = [DiscoveryTask(q, region=args.region) for q in args.queries]
    result = asyncio.run(pool.run(tasks))

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    for k, v in result.items():
        if k == "hits":
            continue
        print(f"  {k:20s} {v}")
    print(f"\n  first 15 hits:")
    for h in result.get("hits", [])[:15]:
        print(f"    {h['item_id']}  {(h.get('title') or '')[:60]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
