"""Deterministic verification helpers for worker claims."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any


FILE_REF_RE = re.compile(r"(?P<path>[A-Za-z0-9_./ -]+\.[A-Za-z0-9_+-]+):(?P<start>\d+)(?:-(?P<end>\d+))?")


def _tokens(text: str) -> set[str]:
    raw = re.findall(
        r"[a-zA-Zа-яА-Я_][a-zA-Zа-яА-Я0-9_]*|\d+(?:\.\d+)+|\d{2,}",
        text.lower(),
    )
    return {token for token in raw if len(token) >= 3 or "." in token}


def _quoted_literals(text: str) -> set[str]:
    return {
        value.strip()
        for value in re.findall(r"[\"'`]([^\"'`]{2,80})[\"'`]", text)
        if value.strip()
    }


def _inside(root: Path, candidate: Path) -> bool:
    return candidate == root or root in candidate.parents


def resolve_evidence_reference(reference: str, cwd: Path) -> dict[str, Any] | None:
    match = FILE_REF_RE.search(reference)
    if not match:
        return None
    raw_path = match.group("path").strip()
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = cwd / candidate
    try:
        candidate = candidate.resolve()
    except OSError:
        return None
    root = cwd.resolve()
    if not _inside(root, candidate) or not candidate.is_file():
        return {"reference": reference, "valid": False, "reason": "file_missing_or_outside_scope"}
    start = int(match.group("start"))
    end = int(match.group("end") or start)
    if start < 1 or end < start or end - start > 20:
        return {"reference": reference, "valid": False, "reason": "invalid_line_range"}
    try:
        lines = candidate.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return {"reference": reference, "valid": False, "reason": "unreadable_file"}
    if start > len(lines):
        return {"reference": reference, "valid": False, "reason": "line_out_of_range"}
    excerpt = "\n".join(lines[start - 1 : min(end, len(lines))])
    return {
        "reference": reference,
        "valid": True,
        "path": str(candidate.relative_to(root)),
        "start": start,
        "end": min(end, len(lines)),
        "excerpt": excerpt,
    }


def verify_claim_locally(claim: dict[str, Any], cwd: Path) -> dict[str, Any]:
    statement = str(claim.get("statement") or claim.get("claim") or "").strip()
    evidence = claim.get("evidence") or []
    if not statement or not isinstance(evidence, list) or not evidence:
        return {"status": "inconclusive", "reason": "missing_statement_or_evidence", "evidence": []}
    checked = [resolve_evidence_reference(str(item), cwd) for item in evidence]
    checked = [item for item in checked if item is not None]
    valid = [item for item in checked if item.get("valid")]
    invalid = [item for item in checked if not item.get("valid")]
    if invalid:
        return {"status": "inconclusive", "reason": "invalid_file_reference", "evidence": checked}
    if not valid:
        return {"status": "inconclusive", "reason": "no_machine_checkable_file_reference", "evidence": []}
    statement_tokens = _tokens(statement)
    quoted_literals = _quoted_literals(statement)
    best_overlap = 0.0
    literal_support = False
    for item in valid:
        excerpt = str(item.get("excerpt") or "")
        excerpt_tokens = _tokens(excerpt)
        if statement_tokens:
            best_overlap = max(best_overlap, len(statement_tokens & excerpt_tokens) / len(statement_tokens))
        if any(literal in excerpt for literal in quoted_literals):
            literal_support = True
    if best_overlap >= 0.22 or (literal_support and best_overlap >= 0.08):
        return {
            "status": "evidence_validated",
            "reason": (
                "file_reference_exists_and_supports_claim_literal"
                if literal_support
                else "file_reference_exists_and_supports_claim_terms"
            ),
            "term_overlap": round(best_overlap, 3),
            "evidence": valid,
        }
    return {
        "status": "inconclusive",
        "reason": "file_reference_exists_but_semantic_support_is_weak",
        "term_overlap": round(best_overlap, 3),
        "evidence": valid,
    }


def needs_independent_verifier(claim: dict[str, Any], local_result: dict[str, Any]) -> bool:
    importance = str(claim.get("importance") or "").lower()
    return importance in {"high", "critical"} and local_result.get("status") != "contradicted"


def verifier_task(statement: str, source_branch_id: str, claim_id: str) -> str:
    return (
        "Independently verify the following delegated-worker claim using the repository itself. "
        "Do not trust the source worker's evidence without checking it. "
        f"Source branch: {source_branch_id}; claim id: {claim_id}. Claim: {statement}\n\n"
        "Your first returned claim statement must be exactly one of: VERDICT: SUPPORTED, "
        "VERDICT: CONTRADICTED, or VERDICT: INCONCLUSIVE. Attach concrete file:line or command evidence."
    )


def verifier_verdict(result: dict[str, Any] | None) -> str | None:
    if not isinstance(result, dict):
        return None
    claims = result.get("claims") or []
    if not isinstance(claims, list):
        return None
    for claim in claims:
        statement = str((claim or {}).get("statement") or "").strip().upper()
        if statement == "VERDICT: SUPPORTED":
            return "verified"
        if statement == "VERDICT: CONTRADICTED":
            return "contradicted"
        if statement == "VERDICT: INCONCLUSIVE":
            return "inconclusive"
    return None
