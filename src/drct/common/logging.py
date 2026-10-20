"""Structured JSON-lines logging to stdout."""

import structlog


def get_logger(name: str, **bound) -> structlog.BoundLogger:
    """Return a logger emitting JSON lines with ``event``, ``level``, ``timestamp`` and bound keys."""
    log = structlog.wrap_logger(
        structlog.PrintLogger(),  # resolves sys.stdout at call time
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
    )
    return log.bind(logger=name, **bound)
