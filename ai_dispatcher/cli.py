"""Command-line entry point: ``python -m ai_dispatcher <command>``.

Commands
--------
* ``verify``           run the CI-parity gate against the repo (exit 0 = green)
* ``validate-packet``  validate a handoff packet's structure
* ``select``           show the next armed task from the brief
* ``loop``             run one bounded dispatch for a task id (optionally publish)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from ai_dispatcher import packets, publish, sentinels
from ai_dispatcher.agents.claude_agent import ClaudeExecutor
from ai_dispatcher.agents.codex_agent import CodexAgent
from ai_dispatcher.config import DispatcherConfig, default_config
from ai_dispatcher.loop import DispatchLoop, DispatchResult
from ai_dispatcher.tasks import SelectionTooLargeError, check_size, parse_tasks, select_next
from ai_dispatcher.verify import is_publishable, run_verify


def _config(args: argparse.Namespace) -> DispatcherConfig:
    return default_config(Path(args.repo_root).resolve())


def _cmd_verify(args: argparse.Namespace) -> int:
    config = _config(args)
    log: list[str] = []
    result = run_verify(config, log_sink=log)
    print("\n".join(log))
    print(f"publishable={is_publishable(result)} steps={result.steps_run}/{result.steps_total}")
    return 0 if result.ok else 1


def _cmd_validate_packet(args: argparse.Namespace) -> int:
    problems = packets.validate_packet(Path(args.packet))
    if not problems:
        print(f"OK: {args.packet} is a valid packet")
        return 0
    print(f"INVALID: {args.packet}")
    for problem in problems:
        print(f"  - {problem}")
    return 1


def _cmd_select(args: argparse.Namespace) -> int:
    config = _config(args)
    if not config.tasks_file.exists():
        print(f"no task brief at {config.tasks_file}")
        return 1
    tasks = parse_tasks(config.tasks_file.read_text(encoding="utf-8"))
    task = select_next(tasks)
    if task is None:
        print("no armed task available")
        return 1
    print(f"{task.task_id}: {task.title}")
    return 0


def _cmd_loop(args: argparse.Namespace) -> int:
    config = _config(args)
    if sentinels.is_stopped(config):
        print("ABORT: operator stop sentinel present")
        return 2
    halt = sentinels.halt_state(config)
    if halt.halted:
        print(f"ABORT: halt sentinel present ({halt.reason})")
        return 2
    if sentinels.breaker_tripped(config):
        print(f"ABORT: consecutive-failure breaker tripped ({config.max_consecutive_failures})")
        return 2

    brief = config.tasks_file.read_text(encoding="utf-8")
    try:
        check_size(brief)
    except SelectionTooLargeError as exc:
        print(f"ABORT: {exc}")
        return 2
    tasks = parse_tasks(brief)
    task = next((t for t in tasks if t.task_id == args.task_id), None)
    if task is None:
        print(f"no task with id {args.task_id!r} in {config.tasks_file}")
        return 1

    planner = CodexAgent(config)
    executor = ClaudeExecutor(config)
    controller = CodexAgent(config)
    loop = DispatchLoop(config, planner=planner, executor=executor, controller=controller)
    result = loop.run(task)
    _print_result(result)

    if result.status != "passed":
        sentinels.bump_consecutive_failures(config)
        return 1
    sentinels.reset_consecutive_failures(config)

    if args.publish != "none":
        return _publish(config, result, mode=args.publish)
    return 0


def _publish(config: DispatcherConfig, result: DispatchResult, *, mode: str) -> int:
    from datetime import date

    authorizations = publish.parse_authorizations(
        config.authorizations_file.read_text(encoding="utf-8")
        if config.authorizations_file.exists()
        else "",
        today=date.today(),
    )
    decision = publish.decide_publish(
        mode,  # type: ignore[arg-type]
        changed_files=list(result.changed_files),
        authorizations=authorizations,
        used_counts=publish.merge_counts(config),
        high_risk_globs=config.high_risk_globs,
        never_automerge_globs=config.never_automerge_globs,
        artifact_globs=config.artifact_globs,
    )
    print(f"publish decision: {decision.action} ({decision.reason})")
    branch = f"ai-dispatch/{result.dispatch_id}"
    publisher = publish.Publisher(config)
    outcome = publisher.publish(
        decision,
        branch=branch,
        commit_message=f"ai-dispatch {result.dispatch_id}: {result.detail}",
        title=f"ai-dispatch {result.dispatch_id}",
        body=result.detail,
        changed_files=result.changed_files,
    )
    print(f"publish outcome: {'ok' if outcome.ok else 'FAILED'} — {outcome.detail}")
    if outcome.action == "merge" and outcome.ok and decision.auth_id is not None:
        publish.record_merge(
            config, auth_id=decision.auth_id, dispatch_id=result.dispatch_id, sha=""
        )
    return 0 if outcome.ok else 1


def _print_result(result: DispatchResult) -> None:
    print(f"dispatch {result.dispatch_id}: {result.status} — {result.detail}")
    if result.changed_files:
        print(f"  changed: {', '.join(result.changed_files)}")
    if result.scope_violations:
        print(f"  scope violations: {', '.join(result.scope_violations)}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ai_dispatcher", description=__doc__)
    parser.add_argument("--repo-root", default=".", help="repository root (default: cwd)")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("verify", help="run the CI-parity verify gate").set_defaults(func=_cmd_verify)

    p_validate = sub.add_parser("validate-packet", help="validate a handoff packet")
    p_validate.add_argument("packet")
    p_validate.set_defaults(func=_cmd_validate_packet)

    sub.add_parser("select", help="show the next armed task").set_defaults(func=_cmd_select)

    p_loop = sub.add_parser("loop", help="run one bounded dispatch")
    p_loop.add_argument("task_id")
    p_loop.add_argument(
        "--publish",
        choices=("none", "branch", "pr", "main"),
        default="none",
        help="publish posture for a passed dispatch (default: none)",
    )
    p_loop.set_defaults(func=_cmd_loop)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    func = args.func
    return int(func(args))


if __name__ == "__main__":
    sys.exit(main())
