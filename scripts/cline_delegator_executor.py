"""Executor abstraction and the built-in Cline CLI executor."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
from typing import Protocol


@dataclass(frozen=True)
class PreparedExecution:
    command: list[str]
    env: dict[str, str]
    guarded_root: Path | None
    requested_thinking: str


class Executor(Protocol):
    name: str

    def prepare(
        self,
        *,
        cwd: Path,
        prompt: str,
        mode: str,
        timeout_seconds: int,
        model: str | None,
    ) -> PreparedExecution: ...

    def terminate(self, proc: subprocess.Popen[object]) -> None: ...


def _git_root_or_cwd(cwd: Path) -> Path:
    probe = subprocess.run(
        ["git", "-C", str(cwd), "rev-parse", "--show-toplevel"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        timeout=10,
        check=False,
    )
    if probe.returncode == 0 and probe.stdout.strip():
        return Path(probe.stdout.strip()).resolve()
    return cwd


def _macos_read_only_command(command: list[str], cwd: Path) -> tuple[list[str], Path]:
    sandbox_exec = Path("/usr/bin/sandbox-exec")
    if not sandbox_exec.is_file():
        raise RuntimeError("analyze mode requires /usr/bin/sandbox-exec for filesystem isolation")
    guarded_root = _git_root_or_cwd(cwd)
    escaped = str(guarded_root).replace("\\", "\\\\").replace('"', '\\"')
    profile = f'(version 1)(allow default)(deny file-write* (subpath "{escaped}"))'
    return [str(sandbox_exec), "-p", profile, *command], guarded_root


class ClineExecutor:
    name = "cline"

    def prepare(
        self,
        *,
        cwd: Path,
        prompt: str,
        mode: str,
        timeout_seconds: int,
        model: str | None,
    ) -> PreparedExecution:
        executable = os.environ.get("CLINE_BIN") or shutil.which("cline")
        if not executable:
            raise RuntimeError("cline executable was not found; set CLINE_BIN or add cline to PATH")
        thinking = os.environ.get("CLINE_DELEGATOR_THINKING", "high").strip().lower()
        if thinking not in {"none", "low", "medium", "high", "xhigh"}:
            raise ValueError("CLINE_DELEGATOR_THINKING must be none, low, medium, high, or xhigh")
        command = [
            executable,
            "--json",
            "--auto-approve",
            "true",
            "--timeout",
            str(timeout_seconds),
            "--retries",
            "3",
            "--compaction",
            "agentic",
            "--thinking",
            thinking,
            "--cwd",
            str(cwd),
        ]
        if model:
            command.extend(["--model", model])
        guarded_root: Path | None = None
        if mode == "analyze":
            command.append("--plan")
        elif mode == "worktree":
            command.append("--worktree")
        else:
            raise ValueError("mode must be 'analyze' or 'worktree'")
        command.append(prompt)
        env = os.environ.copy()
        deny = [
            "sudo *",
            "git push*",
            "npm publish*",
            "pnpm publish*",
            "yarn npm publish*",
            "twine upload*",
            "docker push*",
            "terraform apply*",
            "terraform destroy*",
            "kubectl apply*",
            "kubectl delete*",
            "gh release*",
        ]
        if mode == "analyze":
            deny.extend(["rm *", "mv *", "cp *", "git add*", "git commit*", "git checkout*", "git switch*", "git reset*", "git clean*", "sed -i*"])
        env["CLINE_COMMAND_PERMISSIONS"] = json.dumps({"allow": ["*"], "deny": deny}, separators=(",", ":"), sort_keys=True)
        if mode == "analyze":
            env["PYTHONDONTWRITEBYTECODE"] = "1"
            command, guarded_root = _macos_read_only_command(command, cwd)
        return PreparedExecution(command, env, guarded_root, thinking)

    def terminate(self, proc: subprocess.Popen[object]) -> None:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
            proc.wait(timeout=5)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


_EXECUTORS: dict[str, Executor] = {"cline": ClineExecutor()}


def register_executor(executor: Executor, *, replace: bool = False) -> None:
    """Register another worker backend without changing orchestration code."""
    name = str(getattr(executor, "name", "")).strip().lower()
    if not name:
        raise ValueError("executor.name must be a non-empty string")
    if name in _EXECUTORS and not replace:
        raise ValueError(f"executor already registered: {name}")
    _EXECUTORS[name] = executor


def get_executor(name: str = "cline") -> Executor:
    try:
        return _EXECUTORS[name]
    except KeyError as exc:
        raise ValueError(f"unknown executor: {name}") from exc


def executor_names() -> list[str]:
    return sorted(_EXECUTORS)
