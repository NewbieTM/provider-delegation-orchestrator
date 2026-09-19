# Provider Delegation Orchestrator

Provider Delegation Orchestrator is a local, provider-agnostic orchestration layer for delegating large tasks from a strong orchestrator model to cheaper/free worker models while keeping the orchestrator's context compact and retaining control over correctness.

The project currently ships as a Codex plugin under the historical plugin name `cline-delegator`. It exposes an MCP tool surface plus a Codex skill for automatic delegation of large tasks. Cline CLI is the bundled worker executor today, but the orchestration runtime is designed around pluggable executors rather than a Codex↔Cline-only bridge.

Codex is currently the primary orchestration client through MCP. The orchestration core does not depend on Cline-specific process construction: additional providers/clients can be added behind the executor and protocol boundaries.

## Why it exists

Large codebase audits, research, log triage, architecture reviews, and broad implementation tasks can consume a large amount of orchestrator context. This project lets the orchestrator decompose such work into independently checkable branches, run workers in parallel, and receive compact structured results instead of full transcripts.

The orchestrator remains responsible for the final answer and for checking decision-critical claims.

## Codex plugin

The repository includes a complete Codex plugin package:

- `.codex-plugin/plugin.json` for plugin metadata;
- `.mcp.json` for the MCP server/tool configuration;
- `skills/delegate-to-cline/` for the Codex delegation skill;
- `scripts/cline_delegator_mcp.py` as the local orchestration/MCP runtime.

In Codex, the skill can decide that a task is large enough to delegate, create an orchestration DAG, launch worker branches, collect compact results, run verification/replanning, and return only the useful evidence back to the orchestrator context.

## High-level flow

```text
orchestrator/client
      |
      | plan + task DAG
      v
delegation runtime
  |   scheduler / budgets / retries
  |   durable SQLite state
  |   verification / cache / tracing
  |
  +----> executor A -> worker/provider A
  +----> executor B -> worker/provider B   (future adapter)
  +----> executor C -> worker/provider C   (future adapter)
      |
      v
compact claims + evidence + provenance
      |
      v
orchestrator verification + final synthesis
```

## Main capabilities

- Durable non-blocking worker jobs with heartbeats, cancellation, partial results, retry/fallback telemetry, and crash/orphan detection.
- DAG orchestration with dependencies, parent/child decomposition, atomic same-batch dependency indices, transitive dependency-failure propagation, and admission-controlled fan-out.
- Automatic branch sizing and token reservations:
  - `compact`: 300k worker-token reservation
  - `standard`: 600k
  - `research`: 1M
- Auto-sized orchestration budget with configurable safety margin and a 10M hard cap.
- Default branch timeouts of roughly 15/30/60 minutes by size class, with per-branch overrides.
- Parallel execution with a configurable worker cap (default `3`) and bounded MCP request handling.
- Analyze mode protected by macOS `sandbox-exec`; implementation work uses isolated worktrees.
- Structured compact result contract: summary, claims, evidence, risks, tests, changed files, open questions, and next actions.
- Claim-level verification and independent verifier branches.
- Source-aware semantic cache for trusted results.
- Dynamic replanning (`wait -> verify -> replan/stop`) and bounded replan rounds.
- SQLite WAL persistence, idempotency, recovery of queued work, retention/pruning, and trace history.
- Local read-only dashboard with DAG, timelines, critical path, worker/model telemetry, retries, verification, budgets, and context-reduction estimates.

## Verification model

Worker output is never automatically trusted.

There are intentionally different levels of confidence:

- `unverified`: worker finished, but no independent check has been recorded.
- `evidence_validated`: referenced `file:line` evidence exists and local checks found support. This is not a proof of the claim.
- `partially_verified`: only part of the branch claim set has stronger verification.
- `verified`: all claims were explicitly accepted by the orchestrator-side verification path.
- `contradicted` / `inconclusive`: verification found conflict or insufficient evidence.

For `high` and `critical` claims, local lexical/file validation never produces `verified`. The runtime can launch an independent verifier worker, but a supporting verifier still leaves the claim at `evidence_validated`; the orchestrator must independently inspect the important evidence and explicitly record verification before the branch is trusted or cached.

Worker self-reported confidence is only an uncertainty signal. It is never verification.

## Semantic cache correctness

Only fully `verified` branches are eligible for semantic reuse.

Cache entries are additionally bound to a Git source fingerprint containing:

- current `HEAD`;
- tracked working-tree/index diff relative to `HEAD`;
- untracked file names and contents.

If the repository changes, the old result cannot be reused. Outside a Git repository semantic reuse is disabled rather than risking stale truth.

## Scheduling and budgets

The orchestrator chooses how many branches are useful for the task. The runtime then applies:

- branch-count limits;
- worker-token reservations;
- orchestration-level hard budgets;
- wall-time limits;
- worker concurrency limits;
- dependency waves;
- retries only for retryable failures;
- model fallback where configured.

Blocked branches do not consume worker slots. If a prerequisite fails, failure is propagated transitively through all descendants that can no longer run.

## Context/token savings

Workers may consume substantial tokens internally, but the orchestrator receives compact packets rather than their full transcripts. The runtime records:

- raw worker transcript size/token estimate;
- compact payload estimate;
- estimated context avoided;
- provider-reported worker usage when available.

These metrics demonstrate context compression; they are not a claim about exact Codex billing.

## Observability

The dashboard is localhost-only and read-only. It shows:

- task/orchestration traces;
- DAG and dependency edges;
- active/terminal workers;
- observed critical path (amber/gold);
- concurrency and execution timeline;
- requested/reported model and reasoning metadata;
- retry/fallback history;
- reservation versus observed worker tokens;
- claim verification states;
- context-reduction estimates.

Security headers and `no-store` responses are enabled. The dashboard does not read raw worker transcripts.

## Architecture

Key modules:

```text
scripts/
  cline_delegator_mcp.py          MCP/API orchestration and job lifecycle
  cline_delegator_executor.py     executor protocol + bundled Cline executor
  cline_delegator_db.py           SQLite schema, migration, one-time initialization
  cline_delegator_policy.py       branch sizing, budgets, timeouts, planning
  cline_delegator_verification.py evidence and verifier logic
  cline_delegator_cache.py        verified semantic cache
  cline_delegator_provenance.py   Git source-state fingerprinting
  cline_delegator_replan.py       next-wave generation
  cline_delegator_semantic.py     semantic similarity helpers
  cline_delegator_tools.py        MCP tool schemas
  cline_delegator_dashboard.py    local trace dashboard
tests/
  test_cline_delegator.py
```

The large historical `cline_delegator_mcp.py` is being reduced over time by moving stable concerns into focused modules. Compatibility routes remain hidden from normal tool discovery so existing installations do not break abruptly.

## Adding another worker provider

Implement the `Executor` protocol in `cline_delegator_executor.py` and register the adapter with `register_executor(...)`:

```python
class Executor(Protocol):
    name: str

    def prepare(...): ...
    def terminate(...): ...
```

An executor is responsible for turning the generic task prompt into a concrete process/API invocation and terminating it safely. The DAG scheduler, persistence, verification, cache, budgets, and observability remain shared.

The current registry ships only `cline`; adding another executor is the main integration point for another local/remote provider.

## Current Codex/Cline adapter

The bundled configuration uses:

- Cline CLI as the worker client;
- `cline-free/deepseek-v4.1-flash` as the preferred worker model unless overridden;
- requested reasoning effort `high`;
- maximum three concurrent workers by default.

Environment variables can override model, fallback models, allowed roots, state directory, concurrency, retention, and request-handler limits.

## Important limitations

- In-flight worker processes are not transparently resumed after supervisor loss. Durable state survives; active work is marked `orphaned` and can be retried/replanned.
- Verification is intentionally conservative. `evidence_validated` means evidence location/support was checked, not that arbitrary program semantics were proven.
- Semantic cache reuse requires Git source provenance.
- The bundled executor is currently Cline; provider-agnostic architecture does not mean every provider already has an adapter.
- The project has strong regression coverage for its local use case, but it is not claimed to be industry-wide SOTA without a multi-provider benchmark/fault-injection corpus.

## Development

Run the full regression suite:

```bash
python3 tests/test_cline_delegator.py -q
```

Compile-check all modules:

```bash
python3 -m py_compile scripts/*.py tests/test_cline_delegator.py
```

When developing as a Codex plugin, run the `plugin-creator` validator from the Codex skill installation available on your machine.

## Repository status

This repository is intentionally small and local-first: one orchestration service, one bundled executor, no separate benchmark infrastructure or fleet control plane. The goal is a maintainable personal orchestration tool with strong correctness boundaries, not a distributed platform.
