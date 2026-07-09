"""Task-brief parsing, selection, and size-guard tests."""

from __future__ import annotations

import pytest

from ai_dispatcher.tasks import (
    SelectionTooLargeError,
    check_size,
    parse_tasks,
    select_next,
)

BRIEF = """# brief

## alpha: first task

- STATUS: done

body a

## beta: second task

- STATUS: armed

body b

## gamma: third task

- STATUS: unarmed

body c
"""


def test_parse_tasks_reads_id_title_status() -> None:
    tasks = parse_tasks(BRIEF)
    assert [t.task_id for t in tasks] == ["alpha", "beta", "gamma"]
    assert tasks[1].title == "second task"
    assert tasks[1].armed
    assert not tasks[0].armed


def test_select_next_picks_first_armed_not_done() -> None:
    tasks = parse_tasks(BRIEF)
    assert select_next(tasks) is not None
    assert select_next(tasks).task_id == "beta"  # type: ignore[union-attr]
    assert select_next(tasks, done_ids={"beta"}) is None


def test_check_size_raises_over_ceiling() -> None:
    check_size("small", ceiling=1000)
    with pytest.raises(SelectionTooLargeError):
        check_size("x" * 50, ceiling=10)
