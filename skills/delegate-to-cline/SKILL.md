---
name: delegate-to-cline
description: Delegate large independently checkable exploration, analysis, review, triage, research, or explicitly requested implementation work to local Cline agents when doing so can keep substantial reading and intermediate context out of Codex. Do not use for tiny tasks where orchestration costs more than direct work.
---

# Delegate To Cline

Use the `cline_delegator` MCP tools to offload a well-bounded subtask and retain responsibility for the final answer. Prefer durable non-blocking jobs. Legacy `research_*` and blocking `delegate_to_cline` calls remain server-side compatibility routes but are intentionally not advertised in the public tool list.

## When delegation is worthwhile

Delegate when the subtask is independently checkable and likely to require broad repository exploration, long logs, repetitive extraction, test-failure triage, or a first-pass implementation. Work directly for small edits, a single known-file lookup, or tasks whose full context must remain in Codex.

Prefer one precise delegation over several vague calls. State the objective, scope, required evidence, exclusions, and completion criteria. Do not pass secrets or API keys; Cline uses its existing local configuration.

For a large delegable task, proactively decompose it into independent branches that can be verified separately, submit those branches before waiting, and recursively delegate a branch again when it is still broad enough to create substantial exploration context. Keep the parent Codex context focused on the task tree, compact worker summaries, and independently checked key claims.

Decide based on context cost and decomposability, not whether the task is called research. Good candidates include broad codebase exploration, deep review of an implemented feature, architecture or data-flow analysis, large log or test-failure triage, cross-file business-rule tracing, dependency or configuration audits, external research, and comparisons across several approaches. Prefer orchestration automatically when several independent branches would otherwise make Codex read or retain a large amount of intermediate material.

Do not delegate merely because a task is important or difficult. Work directly when the decisive context is small, tightly coupled, or already known, or when splitting it would lose essential cross-branch reasoning.

When decomposing, minimize overlap. Give each branch a distinct question, scope, or evidence target. If `orchestration_add_branches` reports overlap warnings, sharpen the scopes unless the duplication is intentional for independent cross-checking.

Choose result_detail by branch size: compact for narrow lookups, standard for normal investigations, and research for broad or synthesis-heavy branches. The research value is a legacy verbosity-level name, not a restriction on task type. auto is the default and infers the class from task scope. Broad results may be substantially larger than the compact 1200-character contract when preserving evidence and caveats matters.

For orchestration fan-out, prefer choosing the size class per branch with branch_result_details when the branches differ materially in scope. The default reservation classes are intentionally generous for modern reasoning models: compact = 300k worker tokens, standard = 600k, research = 1M. These are reservations, not expected spend.

Before launching a multi-branch orchestration, use `orchestration_plan` when the decomposition is non-trivial. Treat it as a dry run: inspect inferred size classes, per-branch default timeouts, reservation total, scheduler waves, dependency critical path, concurrency bound, and overlap warnings. Refine the branch plan before spending worker budget when overlap is high or the recommended budget would exceed the hard cap. Its `add_branches_arguments` can be passed into `orchestration_add_branches` together with the orchestration id; in-batch `dependency_indices` are resolved atomically to the created/reused branch ids.

Stop recursive decomposition when a branch is already independently checkable, one worker can reasonably solve it within its class, or further splitting would mostly duplicate context. Split further when the branch contains multiple independent subsystems/questions, would otherwise require a worker to retain a very large amount of unrelated context, or benefits from independent claim checks. The goal is a shallow tree of useful evidence units, not the maximum number of agents.

## Modes

- Use `mode: "analyze"` by default. It starts Cline in plan mode and must not modify the checkout.
- Use `mode: "worktree"` only when the user has requested code changes. Cline works in its own detached worktree; never run it concurrently against the active checkout.

The allowed project roots are enforced by the server. If a path is rejected, do not work around the boundary.

## Result handling

The tool returns a compact result plus measured transcript bytes and estimated context-token savings. The raw NDJSON transcript remains in the run directory and should not be loaded unless the compact result is insufficient or debugging the bridge itself.

For each independent subtask:

1. Call `delegate_submit` and retain its `job_id`.
2. Observe progress with `delegate_events`, passing the returned `next_seq` as `after_seq`; use a bounded `wait_ms` rather than tight polling.
3. Use `delegate_get` for the durable status and heartbeat. Cancel with `delegate_cancel` when the user changes scope or the work is no longer needed.
4. When terminal, call `delegate_result`. A timeout, cancellation, or worker failure can still contain a useful `partial` result.

Multiple read-only jobs may be submitted before waiting. The server enforces its concurrency limit and serializes conflicting worktree jobs for the same checkout.

## Large-task orchestration

For any large delegable task, use the durable tree API rather than managing unrelated job IDs manually:

1. Call orchestration_create with the overall objective, cwd, and a conservative max_depth. Normally omit max_total_worker_tokens: the server auto-sizes the worker-token budget from the branches Codex actually creates, using their reservation classes plus a safety margin. Pass max_total_worker_tokens only when a fixed hard cap is explicitly useful.
2. Decompose the objective yourself into independently checkable branches and submit them together with `orchestration_add_branches`. The server validates the whole batch before launching workers, deduplicates repeated branches through stable idempotency keys/fingerprints, and in analyze mode may reuse a semantically similar cached result only when that earlier result was fully verified. Use `dependency_indices` for dependencies among tasks in that same batch and `dependencies` for already-existing branch ids.
3. Use `orchestration_get` to inspect the whole tree without loading transcripts.
4. If work genuinely depends on earlier findings, pass `dependencies` so the dependent branch stays blocked until its prerequisites complete. Use `parent_branch_id` for decomposition lineage; dependency edges and parentage serve different purposes.
5. If a completed branch is still too broad or reveals a new independent question, call `orchestration_add_branches` with that branch as `parent_branch_id`. Do not recursively expand merely to create more agents.
6. If a terminal branch is materially insufficient because the requested result budget was too small, use `orchestration_escalate_branch` to rerun it one class larger while preserving lineage. Escalate because of an observed insufficiency, not as a generic retry policy; research-class branches cannot escalate further.
7. Fetch `orchestration_packet` when enough branches are terminal. It contains compact structured claims, provenance, verification state, operational metrics, and aggregate savings metrics, never raw worker transcripts.
8. Prefer `orchestration_auto_verify` after a wave completes. Local file:line checks only produce `evidence_validated`, never trusted `verified`. High/critical claims should still receive an independent verifier branch, and even a supporting verifier result remains `evidence_validated` until Codex/orchestrator performs and records its own check with `orchestration_record_verification`. Do not treat worker confidence, lexical overlap, or an independent worker alone as final verification for high/critical claims.
9. Use `orchestration_replan` to inspect a completed wave and derive a semantically deduplicated next wave from unresolved questions or disputed claims. Use `apply: false` to preview and `apply: true` to launch it. For the normal closed loop, call `orchestration_advance`: it implements wait → verify → replan/stop and respects `max_replan_rounds`.
10. Use `orchestration_cancel` when the user changes scope or the task tree is no longer needed; cancellation cascades to active descendants.

The server enforces global orchestration budgets and can stop unreleased or active work when hard wall-time or worker-token budgets are exhausted. Auto worker budgets grow from committed branch reservations with 20% headroom by default and remain bounded by the server hard cap of 10M worker tokens. Ready orchestration branches are admitted only up to the concurrency limit; blocked/queued fan-out does not require one polling supervisor process per branch. It also supports bounded transient retry and model fallback. Do not retry deterministic contract, hook/configuration, or user-timeout failures just to obtain a successful-looking answer.

Plan with the scheduler in mind. Independent branches may run together only up to the configured concurrency bound; dependency waves serialize by design. Prefer dependencies only for real information flow, because unnecessary edges lengthen the critical path. The dashboard/trace reports the observed critical path after execution.

Semantic cache reuse is deliberately verified-only and source-aware: unverified, `evidence_validated`, partially verified, contradicted, or inconclusive worker results are never admitted as reusable truth. Cache entries are bound to a Git source-state fingerprint (HEAD + tracked diff + untracked contents), so a changed checkout cannot reuse a result verified against older source. Cache reuse is disabled outside Git repositories. Set `use_semantic_cache: false` when an independent fresh run is required even if a verified similar result exists.

The runtime has an executor abstraction. `cline` is the built-in executor today; orchestration, verification, cache, and replanning do not depend on Cline-specific process construction. The older `research_*` routes are compatibility aliases and are hidden from normal tool discovery. Prefer `orchestration_*` for all new work.

Keep the top-level context to the objective, tree shape, compact branch results, and the small subset of evidence that Codex verifies directly. Do not load raw NDJSON merely to synthesize the final answer.

## Observability

Every orchestration is also a trace. The server automatically links new orchestrations to the current Codex thread when CODEX_THREAD_ID is available; a real Codex turn id is recorded when the client exposes one. Do not invent thread or turn identifiers.

Use cline_trace_list to find recent traces and cline_trace_get to inspect one trace as a compact DAG plus timeline, worker concurrency, requested/reported model, requested/effective thinking when the provider reports it, retries, verification state, token reservations versus observed usage, critical path, and context-savings metrics. These views are derived from SQLite metadata and compact results and must not load raw worker transcripts. A zero reservation on historical branches means reservation telemetry did not exist for that run; do not interpret it as a real zero-token budget.

When the user asks to watch or visualize delegation, call cline_dashboard_start. It launches a read-only localhost dashboard that refreshes live and shows the orchestration DAG, event timeline, worker states, budget/observed token usage, retry/fallback history, verification details, critical path, filtering, and run comparison. Amber/gold highlighting means the observed critical path: the longest dependency/lineage chain by accumulated worker duration, not a quality score. Return the localhost URL to the user.

The event packet_returned_to_codex means Codex fetched the compact orchestration packet back from the bridge; it does not mean the final Codex answer has been emitted. Keep this distinction when explaining a trace.

After delegation:

1. Check the reported evidence, terminal reason, and status. `completed_unverified` means the worker finished; it does not mean Codex accepted the claims.
2. For implementation work, inspect the resulting diff or worktree and run proportionate verification before accepting it.
3. Inspect retry/fallback history and failure reasons when they are material. A successful fallback does not erase the earlier failure from telemetry.
4. Report the savings metrics as estimates, not exact Codex billing. `cline_reported_tokens` is included only when Cline emits provider usage.
5. Use `cline_savings_report` when the user asks for cumulative savings.

Raw run directories are retained for a bounded period while SQLite metadata and compact results remain durable. `cline_prune` can preview or apply cleanup explicitly; normal server startup also applies the configured retention policy. Treat `CLINE_DELEGATOR_MODEL` as an explicit override and `CLINE_DELEGATOR_PREFERRED_MODEL` as the default model policy; trace output distinguishes the requested model from the model actually reported by Cline.

Codex remains accountable for correctness. A worker result is evidence, not an automatic conclusion; high/critical claims always require orchestrator-side verification before being treated as trusted or cacheable.
