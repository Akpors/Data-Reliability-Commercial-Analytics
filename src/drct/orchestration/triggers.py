"""File-arrival trigger for Airflow event-driven scheduling (``AssetWatcher``)."""

import asyncio
import fnmatch
import os
from collections.abc import AsyncIterator
from typing import Any

from airflow.triggers.base import BaseEventTrigger, TriggerEvent


class FileArrivalTrigger(BaseEventTrigger):
    """Fire when new files matching ``pattern`` land at the top level of ``directory``.

    It runs for as long as the triggerer keeps it. Names already reported are remembered in
    memory, so one landing batch fires one event rather than one per poll; a name that disappears
    (the loader filed it away) is forgotten, so a re-sent file with the same name fires again.
    Sub-folders, dotfiles and non-matching names are ignored, and files are never touched: the
    DAG run loads and moves them. A missing directory is treated as empty.
    """

    def __init__(self, directory: str, pattern: str, poll_interval: float = 5.0) -> None:
        super().__init__()
        self.directory = directory
        self.pattern = pattern
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            f"{type(self).__module__}.{type(self).__qualname__}",
            {
                "directory": self.directory,
                "pattern": self.pattern,
                "poll_interval": self.poll_interval,
            },
        )

    def _matching_files(self) -> set[str]:
        try:
            with os.scandir(self.directory) as entries:
                return {
                    e.name
                    for e in entries
                    if not e.name.startswith(".")
                    and fnmatch.fnmatchcase(e.name, self.pattern)
                    and e.is_file()
                }
        except FileNotFoundError:
            return set()

    async def run(self) -> AsyncIterator[TriggerEvent]:
        seen: set[str] = set()
        while True:
            present = self._matching_files()
            new = present - seen
            seen = present  # forget names that left, so a re-sent file fires again
            if new:
                yield TriggerEvent({"directory": self.directory, "files": sorted(new)})
            await asyncio.sleep(self.poll_interval)
