"""MCP tool schemas exposed by Cline Delegator."""

from __future__ import annotations

from typing import Any

TOOLS = [
    {
        "name": "delegate_submit",
        "description": (
            "Create a durable Cline job and return its job_id immediately. Prefer this over the legacy "
            "blocking delegate_to_cline tool. Worker output remains unverified until Codex checks it."
        ),
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["task", "cwd"],
            "properties": {
                "task": {"type": "string", "minLength": 1, "maxLength": 30000},
                "cwd": {"type": "string", "minLength": 1},
                "mode": {"type": "string", "enum": ["analyze", "worktree"], "default": "analyze"},
                "result_detail": {
                    "type": "string",
                    "enum": ["auto", "compact", "standard", "research"],
                    "default": "auto",
                    "description": "Controls compact result budget; auto infers it from task scope."
                },
                "timeout_seconds": {"type": "integer", "minimum": 30, "maximum": 3600, "default": 900},
                "executor": {"type": "string", "default": "cline", "description": "Execution backend name; currently cline is built in."},
                "model": {"type": "string"},
                "fallback_models": {
                    "type": "array",
                    "maxItems": 8,
                    "items": {"type": "string", "minLength": 1},
                },
                "max_attempts": {"type": "integer", "minimum": 1, "maximum": 8},
                "trace_context": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "codex_thread_id": {"type": "string"},
                        "codex_turn_id": {"type": "string"}
                    }
                },
            },
        },
    },
    {
        "name": "delegate_get",
        "description": "Read durable job status, heartbeat, latest event, and current partial checkpoint.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["job_id"],
            "properties": {"job_id": {"type": "string"}},
        },
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "delegate_events",
        "description": "Read job events after a cursor, optionally waiting up to 30 seconds for progress.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["job_id"],
            "properties": {
                "job_id": {"type": "string"},
                "after_seq": {"type": "integer", "minimum": 0, "default": 0},
                "limit": {"type": "integer", "minimum": 1, "maximum": 500, "default": 100},
                "wait_ms": {"type": "integer", "minimum": 0, "maximum": 30000, "default": 0},
            },
        },
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "delegate_result",
        "description": "Return the durable terminal result, or the latest partial checkpoint while a job is active.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["job_id"],
            "properties": {
                "job_id": {"type": "string"},
                "include_partial": {"type": "boolean", "default": True},
            },
        },
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "delegate_cancel",
        "description": "Request cooperative cancellation of a queued or running Cline job.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["job_id"],
            "properties": {"job_id": {"type": "string"}},
        },
    },
    {
        "name": "orchestration_plan",
        "description": "Dry-run a proposed branch decomposition before launching workers: infer per-branch size/timeout, budget, overlap, scheduler waves, and dependency critical path.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["tasks"],
            "properties": {
                "tasks": {"type": "array", "minItems": 1, "maxItems": 50, "items": {"type": "string", "minLength": 1, "maxLength": 30000}},
                "branch_result_details": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["auto", "compact", "standard", "research"]}
                },
                "result_detail": {"type": "string", "enum": ["auto", "compact", "standard", "research"], "default": "auto"},
                "branch_timeout_seconds": {
                    "type": "array",
                    "items": {"type": "integer", "minimum": 30, "maximum": 3600}
                },
                "dependency_indices": {
                    "type": "array",
                    "items": {"type": "array", "items": {"type": "integer", "minimum": 0}}
                },
                "worker_budget_safety_margin": {"type": "number", "minimum": 0, "maximum": 1, "default": 0.20}
            }
        },
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "orchestration_create",
        "description": "Create a durable orchestration root for any large delegable task tree: research, codebase exploration, deep review, log/test triage, or similar analysis.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["objective", "cwd"],
            "properties": {
                "objective": {"type": "string", "minLength": 1, "maxLength": 30000},
                "cwd": {"type": "string", "minLength": 1},
                "mode": {"type": "string", "enum": ["analyze", "worktree"], "default": "analyze"},
                "result_detail": {"type": "string", "enum": ["auto", "compact", "standard", "research"], "default": "auto"},
                "max_depth": {"type": "integer", "minimum": 1, "maximum": 8, "default": 3},
                "max_branches": {"type": "integer", "minimum": 1, "maximum": 500, "default": 50},
                "max_total_worker_tokens": {
                    "type": "integer",
                    "minimum": 10000,
                    "maximum": 10000000,
                    "description": (
                        "Optional fixed worker-token cap. Omit it to auto-size the orchestration "
                        "budget from the branches Codex actually creates."
                    )
                },
                "worker_budget_safety_margin": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 1,
                    "default": 0.20,
                    "description": "Extra headroom applied when the worker-token budget is auto-sized."
                },
                "max_wall_time_seconds": {"type": "number", "minimum": 60, "maximum": 86400, "default": 7200},
                "max_attempts": {"type": "integer", "minimum": 1, "maximum": 5, "default": 2},
                "adaptive_concurrency": {"type": "boolean", "default": True},
                "max_replan_rounds": {"type": "integer", "minimum": 0, "maximum": 10, "default": 3},
                "trace_context": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "codex_thread_id": {"type": "string"},
                        "codex_turn_id": {"type": "string"}
                    }
                }
            },
        },
    },
    {
        "name": "orchestration_add_branches",
        "description": "Add and immediately submit independent child branches under an orchestration root or existing branch.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["orchestration_id", "tasks"],
            "properties": {
                "orchestration_id": {"type": "string"},
                "parent_branch_id": {"type": "string"},
                "tasks": {"type": "array", "minItems": 1, "maxItems": 50, "items": {"type": "string", "minLength": 1, "maxLength": 30000}},
                "dependencies": {
                    "type": "array",
                    "items": {"type": "array", "items": {"type": "string"}},
                    "description": "Optional dependencies on already-known branch IDs, aligned one-to-one with tasks."
                },
                "dependency_indices": {
                    "type": "array",
                    "items": {"type": "array", "items": {"type": "integer", "minimum": 0}},
                    "description": (
                        "Optional dependencies on tasks in this same batch, by zero-based task index. "
                        "This makes orchestration_plan output directly executable in one atomic batch."
                    )
                },
                "idempotency_keys": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional stable keys aligned one-to-one with tasks. Defaults to a task fingerprint."
                },
                "estimated_worker_tokens": {
                    "type": "array",
                    "items": {"type": "integer", "minimum": 1000},
                    "description": "Optional conservative token reservations aligned one-to-one with tasks."
                },
                "branch_result_details": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "enum": ["auto", "compact", "standard", "research"]
                    },
                    "description": (
                        "Per-branch size classes aligned one-to-one with tasks. "
                        "Default reservations: compact=300k, standard=600k, research=1M."
                    )
                },
                "branch_timeout_seconds": {
                    "type": "array",
                    "items": {"type": "integer", "minimum": 30, "maximum": 3600},
                    "description": (
                        "Optional per-branch timeout overrides aligned one-to-one with tasks. "
                        "When omitted, compact=900s, standard=1800s, research=3600s."
                    )
                },
                "result_detail": {"type": "string", "enum": ["auto", "compact", "standard", "research"]},
                "timeout_seconds": {
                    "type": "integer",
                    "minimum": 30,
                    "maximum": 3600,
                    "description": "Optional global timeout override for every branch in this batch."
                },
                "model": {"type": "string"},
                "executor": {"type": "string", "default": "cline"},
                "fallback_models": {"type": "array", "maxItems": 8, "items": {"type": "string"}},
                "max_attempts": {"type": "integer", "minimum": 1, "maximum": 8},
                "use_semantic_cache": {"type": "boolean", "default": True},
                "semantic_cache_threshold": {"type": "number", "minimum": 0.5, "maximum": 1.0, "default": 0.86}
            },
        },
    },
    {
        "name": "orchestration_escalate_branch",
        "description": "Escalate an insufficient terminal branch one size class (compact→standard→research) as a child branch, preserving lineage and consuming the corresponding auto budget.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["orchestration_id", "branch_id"],
            "properties": {
                "orchestration_id": {"type": "string"},
                "branch_id": {"type": "string"},
                "reason": {"type": "string", "maxLength": 5000}
            }
        },
    },
    {
        "name": "orchestration_get",
        "description": "Read the complete orchestration tree, branch status, and provenance without loading worker transcripts.",
        "inputSchema": {"type": "object", "additionalProperties": False, "required": ["orchestration_id"], "properties": {"orchestration_id": {"type": "string"}}},
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "orchestration_packet",
        "description": "Return compact branch results, provenance, verification state, and aggregate savings metrics for an orchestration; never raw transcripts.",
        "inputSchema": {"type": "object", "additionalProperties": False, "required": ["orchestration_id"], "properties": {"orchestration_id": {"type": "string"}}},
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "orchestration_record_verification",
        "description": "Record orchestrator-side verification for specific worker claims after independently checking their evidence.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["orchestration_id", "branch_id", "claims"],
            "properties": {
                "orchestration_id": {"type": "string"},
                "branch_id": {"type": "string"},
                "claims": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["claim_id", "status"],
                        "properties": {
                            "claim_id": {"type": "string"},
                            "status": {"type": "string", "enum": ["verified", "evidence_validated", "contradicted", "inconclusive"]},
                            "evidence": {"type": "array", "items": {"type": "string"}},
                            "note": {"type": "string"}
                        }
                    }
                }
            }
        }
    },
    {
        "name": "orchestration_auto_verify",
        "description": "Validate machine-checkable evidence locally and launch independent verifier branches for high/critical claims. Local validation never finalizes a high/critical claim as trusted verified.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["orchestration_id"],
            "properties": {
                "orchestration_id": {"type": "string"},
                "spawn_verifiers": {"type": "boolean", "default": True},
                "max_verifiers": {"type": "integer", "minimum": 0, "maximum": 20, "default": 5}
            }
        }
    },
    {
        "name": "orchestration_replan",
        "description": "Inspect a terminal wave, derive a semantically deduplicated next wave from unresolved questions or disputed claims, and optionally launch it.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["orchestration_id"],
            "properties": {
                "orchestration_id": {"type": "string"},
                "apply": {"type": "boolean", "default": False},
                "max_new_branches": {"type": "integer", "minimum": 1, "maximum": 20, "default": 5},
                "similarity_threshold": {"type": "number", "minimum": 0.5, "maximum": 1.0, "default": 0.82}
            }
        }
    },
    {
        "name": "orchestration_advance",
        "description": "Advance the durable plan→execute→verify→replan loop. Waits for active work, verifies a completed wave, then launches the next useful wave or stops.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["orchestration_id"],
            "properties": {
                "orchestration_id": {"type": "string"},
                "spawn_verifiers": {"type": "boolean", "default": True},
                "max_verifiers": {"type": "integer", "minimum": 0, "maximum": 20, "default": 5},
                "apply_replan": {"type": "boolean", "default": True},
                "max_new_branches": {"type": "integer", "minimum": 1, "maximum": 20, "default": 5}
            }
        }
    },
    {
        "name": "orchestration_cancel",
        "description": "Cancel an orchestration and cascade cancellation to descendant jobs.",
        "inputSchema": {"type": "object", "additionalProperties": False, "required": ["orchestration_id"], "properties": {"orchestration_id": {"type": "string"}}},
    },
    {
        "name": "research_create",
        "description": "Compatibility alias for orchestration_create using research_id.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["objective", "cwd"],
            "properties": {
                "objective": {"type": "string", "minLength": 1, "maxLength": 30000},
                "cwd": {"type": "string", "minLength": 1},
                "mode": {"type": "string", "enum": ["analyze", "worktree"], "default": "analyze"},
                "result_detail": {"type": "string", "enum": ["auto", "compact", "standard", "research"], "default": "auto"},
                "max_depth": {"type": "integer", "minimum": 1, "maximum": 8, "default": 3}
            },
        },
    },
    {
        "name": "research_add_branches",
        "description": "Compatibility alias for orchestration_add_branches using research_id.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["research_id", "tasks"],
            "properties": {
                "research_id": {"type": "string"},
                "parent_branch_id": {"type": "string"},
                "tasks": {"type": "array", "minItems": 1, "maxItems": 50, "items": {"type": "string", "minLength": 1, "maxLength": 30000}},
                "result_detail": {"type": "string", "enum": ["auto", "compact", "standard", "research"]},
                "timeout_seconds": {"type": "integer", "minimum": 30, "maximum": 3600, "default": 900}
            },
        },
    },
    {
        "name": "research_get",
        "description": "Compatibility alias for orchestration_get using research_id.",
        "inputSchema": {"type": "object", "additionalProperties": False, "required": ["research_id"], "properties": {"research_id": {"type": "string"}}},
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "research_packet",
        "description": "Compatibility alias for orchestration_packet using research_id.",
        "inputSchema": {"type": "object", "additionalProperties": False, "required": ["research_id"], "properties": {"research_id": {"type": "string"}}},
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "research_cancel",
        "description": "Compatibility alias for orchestration_cancel using research_id.",
        "inputSchema": {"type": "object", "additionalProperties": False, "required": ["research_id"], "properties": {"research_id": {"type": "string"}}},
    },
    {
        "name": "delegate_to_cline",
        "description": (
            "Legacy blocking compatibility wrapper around delegate_submit plus polling. "
            "Use analyze for read-only investigation; use worktree only for user-authorized code changes. "
            "New workflows should use the durable delegate_submit/get/events/result/cancel tools."
        ),
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["task", "cwd"],
            "properties": {
                "task": {"type": "string", "minLength": 1, "maxLength": 30000},
                "cwd": {"type": "string", "minLength": 1},
                "mode": {"type": "string", "enum": ["analyze", "worktree"], "default": "analyze"},
                "result_detail": {
                    "type": "string",
                    "enum": ["auto", "compact", "standard", "research"],
                    "default": "auto"
                },
                "timeout_seconds": {"type": "integer", "minimum": 30, "maximum": 3600, "default": 900},
            },
        },
    },
    {
        "name": "cline_savings_report",
        "description": "Aggregate Cline delegation transcript-compression and token-estimate telemetry.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "last_n": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100},
                "cwd": {"type": "string"},
            },
        },
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "cline_prune",
        "description": (
            "Prune old raw run artifacts while keeping durable SQLite metadata and compact results. "
            "Use dry_run=true to preview reclaimable state."
        ),
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "dry_run": {"type": "boolean", "default": False},
                "raw_retention_days": {"type": "integer", "minimum": 1, "maximum": 3650},
                "max_run_dirs": {"type": "integer", "minimum": 1, "maximum": 100000}
            }
        },
    },
    {
        "name": "cline_run_details",
        "description": "Read the compact saved result and metrics for one Cline delegation run; never returns the raw transcript.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["run_id"],
            "properties": {"run_id": {"type": "string"}},
        },
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "cline_trace_list",
        "description": "List recent orchestration traces with Codex-thread linkage and branch counts for observability.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "last_n": {"type": "integer", "minimum": 1, "maximum": 500, "default": 50},
                "codex_thread_id": {"type": "string"}
            }
        },
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "cline_trace_get",
        "description": "Read one orchestration trace as a compact DAG, timeline, concurrency, verification, and savings view.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["orchestration_id"],
            "properties": {"orchestration_id": {"type": "string"}}
        },
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "cline_dashboard_start",
        "description": "Start the local read-only Cline delegation observability dashboard and return its localhost URL.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "port": {"type": "integer", "minimum": 1024, "maximum": 65535, "default": 8765},
                "host": {"type": "string", "enum": ["127.0.0.1", "localhost"], "default": "127.0.0.1"}
            }
        }
    },
]
