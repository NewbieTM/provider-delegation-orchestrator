"""Pure dynamic-DAG replanning policy."""

from __future__ import annotations

from typing import Any

from cline_delegator_semantic import semantic_similarity


def _is_duplicate(task: str, others: list[str], threshold: float) -> bool:
    return any(semantic_similarity(task, other) >= threshold for other in others)


def build_replan_candidates(
    branches: list[dict[str, Any]],
    existing_tasks: list[str],
    *,
    max_new_branches: int = 5,
    similarity_threshold: float = 0.82,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen_tasks = list(existing_tasks)
    for branch in branches:
        source_branch_id = str(branch.get("branch_id") or "")
        task = str(branch.get("task") or "")
        if task.startswith("Independently verify the following delegated-worker claim"):
            continue
        result = branch.get("result") or {}
        for question in result.get("open_questions") or []:
            question = str(question).strip()
            if not question:
                continue
            proposed = (
                f"Resolve this open question discovered by branch {source_branch_id}: {question}. "
                "Return concrete evidence and explicitly state whether the question is resolved."
            )
            if not _is_duplicate(proposed, seen_tasks, similarity_threshold):
                candidates.append(
                    {
                        "task": proposed,
                        "source_branch_id": source_branch_id,
                        "reason": "open_question",
                        "result_detail": "compact",
                    }
                )
                seen_tasks.append(proposed)
            if len(candidates) >= max_new_branches:
                return candidates
        verification = branch.get("verification") or {}
        result_claims = {
            str(claim.get("claim_id") or ""): str(claim.get("statement") or "")
            for claim in result.get("claims") or []
            if isinstance(claim, dict)
        }
        for item in verification.get("claims") or []:
            status = str(item.get("status") or "")
            if status not in {"contradicted", "inconclusive"}:
                continue
            claim_id = str(item.get("claim_id") or "")
            statement = result_claims.get(claim_id, claim_id)
            proposed = (
                f"Re-investigate the disputed claim from branch {source_branch_id}: {statement}. "
                f"Prior verification status was {status}. Resolve it independently with concrete evidence."
            )
            if not _is_duplicate(proposed, seen_tasks, similarity_threshold):
                candidates.append(
                    {
                        "task": proposed,
                        "source_branch_id": source_branch_id,
                        "reason": f"verification_{status}",
                        "result_detail": "standard" if status == "contradicted" else "compact",
                    }
                )
                seen_tasks.append(proposed)
            if len(candidates) >= max_new_branches:
                return candidates
    return candidates
