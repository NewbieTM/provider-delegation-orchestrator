"""Verified-result semantic cache for Cline Delegator."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any

from cline_delegator_provenance import source_fingerprint
from cline_delegator_semantic import best_similar_task, task_signature


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def find_verified_cache_hit(
    connection: sqlite3.Connection,
    *,
    task: str,
    cwd: str,
    mode: str,
    result_detail: str,
    threshold: float = 0.86,
) -> dict[str, Any] | None:
    fingerprint = source_fingerprint(Path(cwd))
    if fingerprint is None:
        return None
    rows = connection.execute(
        "SELECT * FROM semantic_cache WHERE cwd=? AND mode=? AND result_detail=? "
        "AND source_fingerprint=? AND verification_status='verified' ORDER BY updated_at DESC LIMIT 250",
        (cwd, mode, result_detail, fingerprint),
    ).fetchall()
    candidates = [dict(row) for row in rows]
    hit = best_similar_task(task, candidates, threshold=threshold)
    if hit is None:
        return None
    connection.execute(
        "UPDATE semantic_cache SET hit_count=hit_count+1,updated_at=? WHERE cache_id=?",
        (_now(), hit["cache_id"]),
    )
    try:
        hit["result"] = json.loads(hit["result_json"])
    except (TypeError, json.JSONDecodeError):
        return None
    return hit


def store_verified_cache_entry(
    connection: sqlite3.Connection,
    *,
    task: str,
    cwd: str,
    mode: str,
    result_detail: str,
    result: dict[str, Any],
    source_branch_id: str,
    source_job_id: str,
) -> str:
    fingerprint = source_fingerprint(Path(cwd))
    if fingerprint is None:
        return ""
    signature = task_signature(task)
    cache_id = hashlib.sha256(
        f"{cwd}\0{mode}\0{result_detail}\0{fingerprint}\0{signature}".encode("utf-8")
    ).hexdigest()[:32]
    now = _now()
    connection.execute(
        "INSERT INTO semantic_cache(cache_id,task_text,task_signature,cwd,mode,result_detail,source_fingerprint,result_json,"
        "verification_status,source_branch_id,source_job_id,hit_count,created_at,updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,'verified',?,?,0,?,?) "
        "ON CONFLICT(cache_id) DO UPDATE SET task_text=excluded.task_text,result_json=excluded.result_json,"
        "source_branch_id=excluded.source_branch_id,source_job_id=excluded.source_job_id,"
        "verification_status='verified',updated_at=excluded.updated_at",
        (
            cache_id,
            task,
            signature,
            cwd,
            mode,
            result_detail,
            fingerprint,
            json.dumps(result, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
            source_branch_id,
            source_job_id,
            now,
            now,
        ),
    )
    return cache_id
