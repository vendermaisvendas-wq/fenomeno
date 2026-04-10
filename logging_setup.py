"""
Configuração central de logging.

Formato:
    2026-04-10T14:30:00 [INFO] fb_search.monitor: listing=123 layer=og price=3500

Destino:
    - logs/monitor.log (rotativo, 5 arquivos de 5MB)
    - stderr (apenas WARNING+)

Uso em qualquer módulo:
    from logging_setup import get_logger
    log = get_logger("monitor")
    log.info(f"listing={lid} layer={layer} price={price}")
"""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "monitor.log"
ROOT_NAME = "fb_search"

_configured = False


def configure(verbose: bool = False, quiet_console: bool = False) -> None:
    """Idempotente. Chame uma vez na entrada de cada script CLI."""
    global _configured
    if _configured:
        return

    LOG_DIR.mkdir(exist_ok=True)

    root = logging.getLogger(ROOT_NAME)
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    root.propagate = False

    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=5_000_000, backupCount=5, encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    if not quiet_console:
        stream = logging.StreamHandler(sys.stderr)
        stream.setLevel(logging.WARNING)
        stream.setFormatter(fmt)
        root.addHandler(stream)

    _configured = True


def get_logger(name: str) -> logging.Logger:
    """Retorna um logger filho de `fb_search`. Chama configure() se ainda não
    foi feito (defaults). Módulos de biblioteca podem chamar get_logger sem
    configurar — aí só funciona se o CLI já tiver configurado antes."""
    if not _configured:
        configure()
    return logging.getLogger(f"{ROOT_NAME}.{name}")


def kv(**kwargs) -> str:
    """Formata pares key=value para mensagens estruturadas.
    Uso: log.info(kv(listing=lid, layer='og', price=3500))"""
    parts = []
    for k, v in kwargs.items():
        if v is None:
            continue
        s = str(v)
        if " " in s or "=" in s:
            s = f'"{s}"'
        parts.append(f"{k}={s}")
    return " ".join(parts)
