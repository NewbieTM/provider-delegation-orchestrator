"""Planning and result-size policy for Cline Delegator."""

from __future__ import annotations

import re
from typing import Any


def resolve_result_detail(task: str, requested: str) -> str:
    if requested != "auto":
        return requested
    lowered = task.lower()
    broad_task_signals = (
        "research",
        "исслед",
        "сравни",
        "survey",
        "landscape",
        "alternatives",
        "архитектур",
        "codebase",
        "кодовая баз",
        "deep review",
        "code review",
        "глубокий анализ",
        "анализ кода",
        "trace",
        "triage",
        "аудит",
    )
    if len(task) >= 4000 or sum(signal in lowered for signal in broad_task_signals) >= 2:
        return "research"
    if len(task) >= 1800 or any(signal in lowered for signal in broad_task_signals):
        return "standard"
    return "compact"


def result_contract_instruction(detail: str) -> str:
    if detail == "research":
        return (
            "Keep the summary under 8000 characters and each list to the most useful 40 entries. "
            "Preserve concrete findings, disagreements, caveats, and source/file references needed by the parent orchestrator."
        )
    if detail == "standard":
        return "Keep the summary under 3500 characters and each list to the most useful 24 entries."
    return "Keep the summary under 1200 characters and each list to the most useful 12 entries."


def branch_overlap_warnings(tasks: list[str]) -> list[dict[str, Any]]:
    def words(text: str) -> set[str]:
        return {word for word in re.findall(r"[a-zA-Zа-яА-Я0-9_]+", text.lower()) if len(word) >= 4}

    token_sets = [words(task) for task in tasks]
    warnings: list[dict[str, Any]] = []
    for left in range(len(tasks)):
        for right in range(left + 1, len(tasks)):
            union = token_sets[left] | token_sets[right]
            if not union:
                continue
            score = len(token_sets[left] & token_sets[right]) / len(union)
            if score >= 0.7:
                warnings.append(
                    {
                        "left_index": left,
                        "right_index": right,
                        "similarity": round(score, 2),
                        "message": "branches may overlap substantially; consider sharper scopes/evidence targets",
                    }
                )
    return warnings


def default_branch_token_reservation(task: str, result_detail: str) -> int:
    resolved = resolve_result_detail(task, result_detail)
    if resolved == "research":
        return 1000000
    if resolved == "standard":
        return 600000
    return 300000


def default_branch_timeout(result_detail: str) -> int:
    if result_detail == "research":
        return 3600
    if result_detail == "standard":
        return 1800
    return 900


def next_result_detail(result_detail: str) -> str | None:
    if result_detail == "compact":
        return "standard"
    if result_detail == "standard":
        return "research"
    return None


def _plan_waves(dependencies: list[list[int]]) -> tuple[list[int], list[list[int]]]:
    n = len(dependencies)
    indegree = [0] * n
    children: list[list[int]] = [[] for _ in range(n)]
    for index, deps in enumerate(dependencies):
        for dep in deps:
            if dep < 0 or dep >= n or dep == index:
                raise ValueError(f"dependency index out of range/self-reference at branch {index}: {dep}")
            indegree[index] += 1
            children[dep].append(index)
    wave = [0] * n
    ready = [index for index, value in enumerate(indegree) if value == 0]
    visited = 0
    waves: list[list[int]] = []
    while ready:
        current = list(ready)
        waves.append(current)
        ready = []
        for node in current:
            visited += 1
            for child in children[node]:
                wave[child] = max(wave[child], wave[node] + 1)
                indegree[child] -= 1
                if indegree[child] == 0:
                    ready.append(child)
    if visited != n:
        raise ValueError("dependency_indices contains a cycle")
    return wave, waves
