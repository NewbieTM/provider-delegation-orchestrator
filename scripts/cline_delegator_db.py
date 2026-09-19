"""SQLite connection and schema initialization helpers."""

from __future__ import annotations

from pathlib import Path
import sqlite3
import threading


_SCHEMA_LOCK = threading.Lock()
_INITIALIZED_PATHS: set[Path] = set()
SCHEMA_VERSION = 8


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL,
    task TEXT NOT NULL,
    cwd TEXT NOT NULL,
    mode TEXT NOT NULL,
    result_detail TEXT NOT NULL DEFAULT 'auto',
    timeout_seconds REAL NOT NULL,
    status TEXT NOT NULL,
    verification_status TEXT NOT NULL DEFAULT 'unverified',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    heartbeat_at TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    supervisor_pid INTEGER,
    worker_pid INTEGER,
    exit_code INTEGER,
    termination_reason TEXT,
    run_dir TEXT NOT NULL,
    stdout_path TEXT NOT NULL,
    stderr_path TEXT NOT NULL,
    requested_thinking TEXT,
    selected_model TEXT,
    executor_name TEXT NOT NULL DEFAULT 'cline',
    fallback_models_json TEXT,
    max_attempts INTEGER NOT NULL DEFAULT 1,
    attempt_count INTEGER NOT NULL DEFAULT 1,
    cumulative_worker_tokens INTEGER NOT NULL DEFAULT 0,
    cline_json TEXT,
    partial_json TEXT,
    result_json TEXT,
    error TEXT,
    codex_thread_id TEXT,
    codex_turn_id TEXT
);
CREATE TABLE IF NOT EXISTS events (
    job_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    timestamp TEXT NOT NULL,
    type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (job_id, seq),
    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS researches (
    research_id TEXT PRIMARY KEY,
    objective TEXT NOT NULL,
    cwd TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'analyze',
    result_detail TEXT NOT NULL DEFAULT 'auto',
    max_depth INTEGER NOT NULL DEFAULT 3,
    max_branches INTEGER NOT NULL DEFAULT 50,
    max_total_worker_tokens INTEGER NOT NULL DEFAULT 600000,
    auto_worker_budget INTEGER NOT NULL DEFAULT 1,
    worker_budget_safety_margin REAL NOT NULL DEFAULT 0.20,
    max_wall_time_seconds REAL NOT NULL DEFAULT 7200,
    max_attempts INTEGER NOT NULL DEFAULT 2,
    adaptive_concurrency INTEGER NOT NULL DEFAULT 1,
    replan_round INTEGER NOT NULL DEFAULT 0,
    max_replan_rounds INTEGER NOT NULL DEFAULT 3,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    cancelled_at TEXT,
    codex_thread_id TEXT,
    codex_turn_id TEXT
);
CREATE TABLE IF NOT EXISTS research_branches (
    branch_id TEXT PRIMARY KEY,
    research_id TEXT NOT NULL,
    parent_branch_id TEXT,
    job_id TEXT NOT NULL UNIQUE,
    task TEXT NOT NULL,
    depth INTEGER NOT NULL,
    idempotency_key TEXT,
    task_fingerprint TEXT,
    depends_on_json TEXT,
    verification_json TEXT,
    estimated_worker_tokens INTEGER NOT NULL DEFAULT 0,
    cache_source_branch_id TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (research_id) REFERENCES researches(research_id) ON DELETE CASCADE,
    FOREIGN KEY (parent_branch_id) REFERENCES research_branches(branch_id) ON DELETE CASCADE,
    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS trace_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trace_id TEXT NOT NULL,
    research_id TEXT,
    job_id TEXT,
    timestamp TEXT NOT NULL,
    type TEXT NOT NULL,
    payload_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS semantic_cache (
    cache_id TEXT PRIMARY KEY,
    task_text TEXT NOT NULL,
    task_signature TEXT NOT NULL,
    cwd TEXT NOT NULL,
    mode TEXT NOT NULL,
    result_detail TEXT NOT NULL,
    source_fingerprint TEXT,
    result_json TEXT NOT NULL,
    verification_status TEXT NOT NULL,
    source_branch_id TEXT,
    source_job_id TEXT,
    hit_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS verification_tasks (
    verifier_branch_id TEXT PRIMARY KEY,
    research_id TEXT NOT NULL,
    source_branch_id TEXT NOT NULL,
    claim_id TEXT NOT NULL,
    claim_statement TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs(status, created_at);
CREATE INDEX IF NOT EXISTS idx_events_job_seq ON events(job_id, seq);
CREATE INDEX IF NOT EXISTS idx_research_branches_research ON research_branches(research_id, depth, created_at);
CREATE INDEX IF NOT EXISTS idx_research_branches_parent ON research_branches(parent_branch_id);
CREATE INDEX IF NOT EXISTS idx_trace_events_trace_time ON trace_events(trace_id, timestamp, id);
CREATE INDEX IF NOT EXISTS idx_trace_events_research ON trace_events(research_id, timestamp, id);
CREATE INDEX IF NOT EXISTS idx_verification_tasks_source
    ON verification_tasks(research_id, source_branch_id, claim_id);
"""


def _ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(SCHEMA_SQL)
    columns = {row["name"] for row in connection.execute("PRAGMA table_info(jobs)")}
    if "result_detail" not in columns:
        connection.execute("ALTER TABLE jobs ADD COLUMN result_detail TEXT NOT NULL DEFAULT 'auto'")
    job_migrations = {
        "selected_model": "TEXT",
        "executor_name": "TEXT NOT NULL DEFAULT 'cline'",
        "fallback_models_json": "TEXT",
        "max_attempts": "INTEGER NOT NULL DEFAULT 1",
        "attempt_count": "INTEGER NOT NULL DEFAULT 1",
        "cumulative_worker_tokens": "INTEGER NOT NULL DEFAULT 0",
        "codex_thread_id": "TEXT",
        "codex_turn_id": "TEXT",
    }
    for name, ddl in job_migrations.items():
        if name not in columns:
            connection.execute(f"ALTER TABLE jobs ADD COLUMN {name} {ddl}")

    research_columns = {row["name"] for row in connection.execute("PRAGMA table_info(researches)")}
    research_migrations = {
        "max_branches": "INTEGER NOT NULL DEFAULT 50",
        "max_total_worker_tokens": "INTEGER NOT NULL DEFAULT 600000",
        "auto_worker_budget": "INTEGER NOT NULL DEFAULT 0",
        "worker_budget_safety_margin": "REAL NOT NULL DEFAULT 0.20",
        "max_wall_time_seconds": "REAL NOT NULL DEFAULT 7200",
        "max_attempts": "INTEGER NOT NULL DEFAULT 2",
        "adaptive_concurrency": "INTEGER NOT NULL DEFAULT 1",
        "replan_round": "INTEGER NOT NULL DEFAULT 0",
        "max_replan_rounds": "INTEGER NOT NULL DEFAULT 3",
        "codex_thread_id": "TEXT",
        "codex_turn_id": "TEXT",
    }
    for name, ddl in research_migrations.items():
        if name not in research_columns:
            connection.execute(f"ALTER TABLE researches ADD COLUMN {name} {ddl}")

    branch_columns = {row["name"] for row in connection.execute("PRAGMA table_info(research_branches)")}
    branch_migrations = {
        "idempotency_key": "TEXT",
        "task_fingerprint": "TEXT",
        "depends_on_json": "TEXT",
        "verification_json": "TEXT",
        "estimated_worker_tokens": "INTEGER NOT NULL DEFAULT 0",
        "cache_source_branch_id": "TEXT",
    }
    for name, ddl in branch_migrations.items():
        if name not in branch_columns:
            connection.execute(f"ALTER TABLE research_branches ADD COLUMN {name} {ddl}")

    cache_columns = {row["name"] for row in connection.execute("PRAGMA table_info(semantic_cache)")}
    if "source_fingerprint" not in cache_columns:
        connection.execute("ALTER TABLE semantic_cache ADD COLUMN source_fingerprint TEXT")
    connection.execute("DROP INDEX IF EXISTS idx_semantic_cache_scope")
    connection.execute(
        "CREATE INDEX idx_semantic_cache_scope "
        "ON semantic_cache(cwd, mode, result_detail, source_fingerprint, updated_at)"
    )

    connection.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_research_branch_idempotency "
        "ON research_branches(research_id, idempotency_key) WHERE idempotency_key IS NOT NULL"
    )


def connect_database(path: Path) -> sqlite3.Connection:
    """Open a configured connection and migrate a DB only when its schema version is stale."""
    resolved = path.expanduser().resolve()
    connection = sqlite3.connect(str(resolved), timeout=30, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute("PRAGMA foreign_keys=ON")
    if resolved not in _INITIALIZED_PATHS:
        with _SCHEMA_LOCK:
            if resolved not in _INITIALIZED_PATHS:
                current_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
                if current_version > SCHEMA_VERSION:
                    connection.close()
                    raise RuntimeError(
                        f"database schema version {current_version} is newer than supported {SCHEMA_VERSION}"
                    )
                if current_version < SCHEMA_VERSION:
                    _ensure_schema(connection)
                    connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                _INITIALIZED_PATHS.add(resolved)
                try:
                    resolved.chmod(0o600)
                except OSError:
                    pass
    return connection
