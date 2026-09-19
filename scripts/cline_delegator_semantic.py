"""Semantic-lite task similarity helpers for Cline Delegator."""

from __future__ import annotations

from difflib import SequenceMatcher
import hashlib
import re
from typing import Any


def _tokens(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-zA-Zа-яА-Я0-9_]+", text.lower())
        if len(token) >= 3
    }


def task_signature(text: str) -> str:
    normalized = " ".join(sorted(_tokens(text)))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def semantic_similarity(left: str, right: str) -> float:
    left_tokens, right_tokens = _tokens(left), _tokens(right)
    union = left_tokens | right_tokens
    jaccard = len(left_tokens & right_tokens) / len(union) if union else 1.0
    sequence = SequenceMatcher(None, left.lower(), right.lower()).ratio()
    return round(0.7 * jaccard + 0.3 * sequence, 4)


def best_similar_task(
    task: str,
    candidates: list[dict[str, Any]],
    threshold: float = 0.86,
) -> dict[str, Any] | None:
    best: dict[str, Any] | None = None
    best_score = threshold
    for candidate in candidates:
        candidate_text = str(candidate.get("task_text") or candidate.get("task") or "")
        score = semantic_similarity(task, candidate_text)
        if score >= best_score:
            best_score = score
            best = {**candidate, "similarity": score}
    return best
