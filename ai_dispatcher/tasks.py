"""Task source: parse the human-armed task brief and select the next task.

The brief (``ai_dispatcher/dispatch.tasks.md``) is the authorized source of
work. Each task is a ``## <id>: <title>`` section carrying a ``- STATUS:``
line. Only ``armed`` tasks are selectable; ``done``/``unarmed``/blocked tasks
are skipped. A context-size guard rejects a brief that has grown past the
model's input ceiling — the RGE brief silently bricked every tick once it
crossed Codex's ~1 MiB limit.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

#: Conservative selection-prompt ceiling (Codex's is ~1 MiB); keep margin.
DEFAULT_SIZE_CEILING = 900_000

_HEADING_RE = re.compile(
    r"^##[ \t]+(?P<id>[A-Za-z0-9][A-Za-z0-9_.\-]*)[ \t]*:[ \t]*(?P<title>.+?)[ \t]*$"
)
_STATUS_RE = re.compile(r"^-[ \t]+STATUS:[ \t]*(?P<status>[A-Za-z_\-]+)", re.MULTILINE)

_SELECTABLE = "armed"


class SelectionTooLargeError(RuntimeError):
    """The task brief exceeds the model input ceiling and must be archived."""


@dataclass(frozen=True)
class Task:
    task_id: str
    title: str
    body: str
    status: str

    @property
    def armed(self) -> bool:
        return self.status.lower() == _SELECTABLE


def parse_tasks(text: str) -> list[Task]:
    """Parse the brief into tasks (heading + body until the next heading)."""
    lines = text.splitlines()
    tasks: list[Task] = []
    current_id: str | None = None
    current_title = ""
    buffer: list[str] = []

    def flush() -> None:
        if current_id is None:
            return
        body = "\n".join(buffer).strip()
        status_match = _STATUS_RE.search(body)
        status = status_match.group("status").lower() if status_match else "unarmed"
        tasks.append(Task(task_id=current_id, title=current_title, body=body, status=status))

    for line in lines:
        heading = _HEADING_RE.match(line)
        if heading is not None:
            flush()
            current_id = heading.group("id")
            current_title = heading.group("title")
            buffer = []
        elif current_id is not None:
            buffer.append(line)
    flush()
    return tasks


def check_size(text: str, *, ceiling: int = DEFAULT_SIZE_CEILING) -> None:
    """Raise :class:`SelectionTooLargeError` if the brief is too big to embed."""
    size = len(text.encode("utf-8"))
    if size > ceiling:
        raise SelectionTooLargeError(
            f"task brief is {size} bytes, over the {ceiling}-byte selection ceiling; "
            "archive completed entries to ai_dispatcher/dispatch.tasks.archive.md"
        )


def select_next(tasks: Iterable[Task], *, done_ids: Iterable[str] = ()) -> Task | None:
    """Return the first armed task not already completed, or None."""
    done = set(done_ids)
    for task in tasks:
        if task.armed and task.task_id not in done:
            return task
    return None
