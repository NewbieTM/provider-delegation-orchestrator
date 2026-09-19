import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import time
import unittest


SERVER_PATH = Path(__file__).parents[1] / "scripts" / "cline_delegator_mcp.py"
SPEC = importlib.util.spec_from_file_location("cline_delegator_mcp", SERVER_PATH)
SERVER = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(SERVER)


class ClineDelegatorTests(unittest.TestCase):
    def test_existing_v04_database_migrates_before_idempotency_index(self):
        saved = os.environ.get("CLINE_DELEGATOR_STATE_DIR")
        try:
            with tempfile.TemporaryDirectory(dir="/private/tmp") as directory:
                os.environ["CLINE_DELEGATOR_STATE_DIR"] = directory
                path = Path(directory) / "jobs.sqlite3"
                connection = sqlite3.connect(path)
                try:
                    connection.executescript(
                        """
                        CREATE TABLE researches (
                            research_id TEXT PRIMARY KEY,
                            objective TEXT NOT NULL,
                            cwd TEXT NOT NULL,
                            mode TEXT NOT NULL DEFAULT 'analyze',
                            result_detail TEXT NOT NULL DEFAULT 'auto',
                            max_depth INTEGER NOT NULL DEFAULT 3,
                            status TEXT NOT NULL DEFAULT 'active',
                            created_at TEXT NOT NULL,
                            updated_at TEXT NOT NULL,
                            cancelled_at TEXT
                        );
                        CREATE TABLE research_branches (
                            branch_id TEXT PRIMARY KEY,
                            research_id TEXT NOT NULL,
                            parent_branch_id TEXT,
                            job_id TEXT NOT NULL UNIQUE,
                            task TEXT NOT NULL,
                            depth INTEGER NOT NULL,
                            created_at TEXT NOT NULL
                        );
                        """
                    )
                    connection.commit()
                finally:
                    connection.close()
                migrated = SERVER.db_connect()
                try:
                    branch_columns = {
                        row["name"]
                        for row in migrated.execute("PRAGMA table_info(research_branches)")
                    }
                    research_columns = {
                        row["name"]
                        for row in migrated.execute("PRAGMA table_info(researches)")
                    }
                    indexes = {
                        row["name"]
                        for row in migrated.execute("PRAGMA index_list(research_branches)")
                    }
                    cache_columns = {
                        row["name"]
                        for row in migrated.execute("PRAGMA table_info(semantic_cache)")
                    }
                finally:
                    migrated.close()
                self.assertIn("idempotency_key", branch_columns)
                self.assertIn("estimated_worker_tokens", branch_columns)
                self.assertIn("max_branches", research_columns)
                self.assertIn("idx_research_branch_idempotency", indexes)
                self.assertIn("source_fingerprint", cache_columns)
        finally:
            if saved is None:
                os.environ.pop("CLINE_DELEGATOR_STATE_DIR", None)
            else:
                os.environ["CLINE_DELEGATOR_STATE_DIR"] = saved

    def test_contract_is_extracted_from_ndjson(self):
        result = {
            "summary": "Found the cause",
            "evidence": ["app.py:10"],
            "files_changed": [],
            "tests": ["3 passed"],
            "risks": [],
            "next_action": "Patch it",
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "out.ndjson"
            path.write_text(
                json.dumps(
                    {
                        "type": "agent_event",
                        "event": {
                            "text": SERVER.RESULT_START + json.dumps(result) + SERVER.RESULT_END
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            events, texts = SERVER.parse_ndjson(path)
            parsed, found = SERVER.extract_contract(texts)
        self.assertEqual(len(events), 1)
        self.assertTrue(found)
        self.assertEqual(parsed["summary"], "Found the cause")
        self.assertEqual(parsed["evidence"], ["app.py:10"])

    def test_fallback_uses_last_substantive_text(self):
        parsed, found = SERVER.extract_contract(["short", "This is the final useful explanation."])
        self.assertFalse(found)
        self.assertEqual(parsed["summary"], "This is the final useful explanation.")

    def test_incremental_ndjson_reader_only_returns_new_events(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.ndjson"
            path.write_bytes(b'{"type":"one","text":"first"}\n{"type":"tw')
            offset, pending, events, texts = SERVER.read_ndjson_increment(path, 0, b"")
            self.assertEqual([event["type"] for event in events], ["one"])
            self.assertIn("first", texts)
            with path.open("ab") as handle:
                handle.write(b'o","text":"second"}\n')
            new_offset, pending, events, texts = SERVER.read_ndjson_increment(path, offset, pending)
            self.assertGreater(new_offset, offset)
            self.assertEqual([event["type"] for event in events], ["two"])
            self.assertIn("second", texts)
            self.assertEqual(pending, b"")

    def test_payload_reports_reduction(self):
        payload = SERVER.finalize_payload({"status": "completed"}, "x" * 8000, None)
        self.assertGreater(payload["metrics"]["context_tokens_avoided_est"], 0)
        self.assertGreater(payload["metrics"]["context_reduction_percent_est"], 50)

    def test_camel_case_cline_usage_is_captured(self):
        usage = SERVER.find_usage(
            [
                {
                    "type": "run_result",
                    "usage": {
                        "inputTokens": 264942,
                        "outputTokens": 7317,
                        "cacheReadTokens": 190362,
                        "cacheWriteTokens": 0,
                        "totalCost": 0,
                    },
                }
            ]
        )
        self.assertEqual(usage["input_tokens"], 264942)
        self.assertEqual(usage["output_tokens"], 7317)
        self.assertEqual(usage["total_tokens"], 272259)
        self.assertEqual(usage["cache_read_tokens"], 190362)
        self.assertEqual(usage["total_cost"], 0.0)

    def test_tools_list_contains_public_tools_but_hides_legacy_compatibility_routes(self):
        response = SERVER.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
        names = [item["name"] for item in response["result"]["tools"]]
        self.assertEqual(
            names,
            [
                "delegate_submit",
                "delegate_get",
                "delegate_events",
                "delegate_result",
                "delegate_cancel",
                "orchestration_plan",
                "orchestration_create",
                "orchestration_add_branches",
                "orchestration_escalate_branch",
                "orchestration_get",
                "orchestration_packet",
                "orchestration_record_verification",
                "orchestration_auto_verify",
                "orchestration_replan",
                "orchestration_advance",
                "orchestration_cancel",
                "cline_savings_report",
                "cline_prune",
                "cline_run_details",
                "cline_trace_list",
                "cline_trace_get",
                "cline_dashboard_start",
            ],
        )
        config = json.loads((SERVER_PATH.parents[1] / ".mcp.json").read_text(encoding="utf-8"))
        configured = set(config["mcpServers"]["cline_delegator"]["tools"])
        self.assertEqual(configured, set(names))
        self.assertTrue(SERVER.legacy_delegate)
        self.assertTrue(SERVER.create_research)

    def test_mcp_request_executor_is_bounded(self):
        self.assertGreaterEqual(SERVER.REQUEST_EXECUTOR._max_workers, 2)
        self.assertLessEqual(SERVER.REQUEST_EXECUTOR._max_workers, 32)
        self.assertGreaterEqual(SERVER.REQUEST_CAPACITY, SERVER.REQUEST_WORKERS)
        self.assertLessEqual(SERVER.REQUEST_CAPACITY, 256)

    def test_analyze_command_has_os_write_guard(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as directory:
            prepared = SERVER.get_executor("cline").prepare(
                cwd=Path(directory),
                prompt=SERVER.build_prompt("inspect only", "analyze", "auto"),
                mode="analyze",
                timeout_seconds=60,
                model=None,
            )
            command, env = prepared.command, prepared.env
        self.assertEqual(command[0], "/usr/bin/sandbox-exec")
        self.assertIn("deny file-write", command[2])
        self.assertIn("--plan", command)
        self.assertEqual(command[command.index("--thinking") + 1], "high")
        self.assertEqual(prepared.requested_thinking, "high")
        self.assertEqual(env["PYTHONDONTWRITEBYTECODE"], "1")
        self.assertEqual(prepared.guarded_root, Path(directory).resolve())

    def test_worktree_mode_denies_external_publish_side_effects(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as directory:
            prepared = SERVER.get_executor("cline").prepare(
                cwd=Path(directory),
                prompt=SERVER.build_prompt("implement locally", "worktree", "auto"),
                mode="worktree",
                timeout_seconds=60,
                model=None,
            )
            command, env = prepared.command, prepared.env
        permissions = json.loads(env["CLINE_COMMAND_PERMISSIONS"])
        self.assertIn("git push*", permissions["deny"])
        self.assertIn("npm publish*", permissions["deny"])
        self.assertIn("terraform apply*", permissions["deny"])
        self.assertNotIn("--plan", command)
        self.assertIsNone(prepared.guarded_root)

    def test_run_metadata_records_model_and_requested_thinking(self):
        metadata = SERVER.find_run_metadata(
            [
                {
                    "type": "run_result",
                    "finishReason": "completed",
                    "model": {
                        "id": "cline-free/deepseek-v4.1-flash",
                        "provider": "cline",
                        "info": {
                            "reasoningOptions": [
                                {"type": "effort", "values": ["low", "high", "max"]}
                            ]
                        },
                    },
                }
            ],
            "high",
        )
        self.assertEqual(metadata["model"], "cline-free/deepseek-v4.1-flash")
        self.assertEqual(metadata["requested_thinking"], "high")
        self.assertIsNone(metadata["effective_thinking"])
        self.assertEqual(metadata["supported_thinking"], ["low", "high", "max"])
        self.assertEqual(metadata["finish_reason"], "completed")

    def test_research_result_detail_expands_contract_budget(self):
        prompt = SERVER.build_prompt("Compare several orchestration approaches", "analyze", "research")
        self.assertIn("8000 characters", prompt)
        self.assertIn("40 entries", prompt)

    def test_auto_result_detail_detects_research_task(self):
        detail = SERVER.resolve_result_detail(
            "Research and compare the architecture alternatives for a provider bridge", "auto"
        )
        self.assertEqual(detail, "research")

    def test_auto_result_detail_detects_large_codebase_analysis(self):
        detail = SERVER.resolve_result_detail(
            "Perform a deep review of this codebase architecture and trace the data flow", "auto"
        )
        self.assertEqual(detail, "research")

    def test_server_discover_advertises_mcp_tasks_extension(self):
        response = SERVER.handle_request(
            {"jsonrpc": "2.0", "id": 1, "method": "server/discover", "params": {}}
        )
        self.assertIn(
            "io.modelcontextprotocol/tasks",
            response["result"]["capabilities"]["extensions"],
        )


class DurableJobTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir="/private/tmp")
        self.root = Path(self.temporary.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        (self.repo / "tracked.txt").write_text("initial\n", encoding="utf-8")
        subprocess.run(["git", "init", "-q", str(self.repo)], check=True)
        subprocess.run(["git", "-C", str(self.repo), "add", "tracked.txt"], check=True)
        subprocess.run(
            [
                "git",
                "-C",
                str(self.repo),
                "-c",
                "user.name=Cline Delegator Tests",
                "-c",
                "user.email=tests@example.invalid",
                "commit",
                "-q",
                "-m",
                "fixture",
            ],
            check=True,
        )
        self.fake_cline = self.root / "fake-cline"
        self.fake_cline.write_text(
            """#!/usr/bin/env python3
import json
import re
import sys
import time

prompt = sys.argv[-1]
scenario = re.search(r"SCENARIO=([a-z_]+)", prompt)
scenario = scenario.group(1) if scenario else "success"
sleep_match = re.search(r"SLEEP=([0-9.]+)", prompt)
delay = float(sleep_match.group(1)) if sleep_match else 0.05

partial = {"type": "agent_event", "event": {"text": "partial evidence before timeout"}}
print(json.dumps(partial), flush=True)

if scenario == "timeout":
    time.sleep(5)
    raise SystemExit(0)
if scenario == "hook_failure":
    print("session.hook requires a valid hook event payload", file=sys.stderr, flush=True)
    raise SystemExit(1)
if scenario == "model_fallback":
    model = sys.argv[sys.argv.index("--model") + 1] if "--model" in sys.argv else ""
    if model == "primary-model":
        print("503 temporarily unavailable", file=sys.stderr, flush=True)
        raise SystemExit(1)

time.sleep(delay)
result = {
    "summary": "fake worker completed",
    "claims": [{
        "claim_id": "claim-1",
        "statement": "fixture proves the worker completed",
        "evidence": ["fixture:1"],
        "confidence": 0.9,
        "importance": "high",
    }],
    "evidence": ["fixture:1"],
    "files_changed": [],
    "tests": ["fake check passed"],
    "risks": [],
    "next_action": "verify independently",
}
text = "<CLINE_DELEGATION_RESULT>" + json.dumps(result) + "</CLINE_DELEGATION_RESULT>"
print(json.dumps({"type": "agent_event", "event": {"text": text}}), flush=True)
print(json.dumps({
    "type": "run_result",
    "finishReason": "completed",
    "usage": {"inputTokens": 10, "outputTokens": 5, "totalCost": 0},
    "model": {"id": "cline-free/deepseek-v4.1-flash", "provider": "cline"},
}), flush=True)
""",
            encoding="utf-8",
        )
        self.fake_cline.chmod(0o755)
        self.saved_environment = os.environ.copy()
        os.environ.update(
            {
                "CLINE_DELEGATOR_STATE_DIR": str(self.root / "state"),
                "CLINE_DELEGATOR_ALLOWED_ROOTS": str(self.root),
                "CLINE_DELEGATOR_MAX_CONCURRENT": "3",
                "CLINE_DELEGATOR_MIN_TIMEOUT_SECONDS": "0.1",
                "CLINE_DELEGATOR_THINKING": "high",
                "CLINE_BIN": str(self.fake_cline),
            }
        )
        SERVER.db_connect().close()

    def tearDown(self):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            connection = SERVER.db_connect()
            try:
                active = connection.execute(
                    "SELECT COUNT(*) AS count FROM jobs WHERE status NOT IN "
                    "('completed_unverified','verified','failed','timed_out','cancelled','orphaned')"
                ).fetchone()["count"]
            finally:
                connection.close()
            if not active:
                break
            time.sleep(0.1)
        SERVER.reap_supervisors(wait=True)
        os.environ.clear()
        os.environ.update(self.saved_environment)
        self.temporary.cleanup()

    def wait_terminal(self, job_id, timeout=10):
        deadline = time.monotonic() + timeout
        row = None
        while time.monotonic() < deadline:
            row = SERVER.job_row(job_id)
            if row["status"] in SERVER.TERMINAL_STATUSES:
                return row
            time.sleep(0.1)
        self.fail(
            f"job did not finish: {job_id}; status={row['status'] if row else None}; "
            f"supervisor_pid={row['supervisor_pid'] if row else None}; "
            f"worker_pid={row['worker_pid'] if row else None}; "
            f"heartbeat_at={row['heartbeat_at'] if row else None}"
        )

    def test_three_submits_return_immediately_and_run_with_limit(self):
        started = time.monotonic()
        jobs = [
            SERVER.submit_job(
                {
                    "task": f"SCENARIO=success SLEEP=0.6 job={index}",
                    "cwd": str(self.repo),
                    "mode": "analyze",
                    "timeout_seconds": 5,
                }
            )
            for index in range(3)
        ]
        self.assertLess(time.monotonic() - started, 1.0)
        self.assertEqual(len({item["job_id"] for item in jobs}), 3)
        deadline = time.monotonic() + 3
        max_running = 0
        while time.monotonic() < deadline:
            connection = SERVER.db_connect()
            try:
                running = connection.execute(
                    "SELECT COUNT(*) AS count FROM jobs WHERE status = 'running'"
                ).fetchone()["count"]
            finally:
                connection.close()
            max_running = max(max_running, int(running))
            if max_running >= 3:
                break
            time.sleep(0.03)
        self.assertEqual(max_running, 3)
        rows = [self.wait_terminal(item["job_id"]) for item in jobs]
        self.assertTrue(all(row["status"] == "completed_unverified" for row in rows))
        self.assertTrue(all(json.loads(row["result_json"])["verification_status"] == "unverified" for row in rows))

    def test_timeout_preserves_partial_result(self):
        job = SERVER.submit_job(
            {
                "task": "SCENARIO=timeout",
                "cwd": str(self.repo),
                "mode": "worktree",
                "timeout_seconds": 0.5,
            }
        )
        row = self.wait_terminal(job["job_id"])
        self.assertEqual(row["status"], "timed_out")
        payload = SERVER.get_job_result({"job_id": job["job_id"]})
        self.assertTrue(payload["partial"])
        self.assertIn("partial evidence", payload["result"]["summary"])

    def test_hook_failure_is_classified_and_partial_is_kept(self):
        job = SERVER.submit_job(
            {
                "task": "SCENARIO=hook_failure",
                "cwd": str(self.repo),
                "mode": "worktree",
                "timeout_seconds": 5,
            }
        )
        row = self.wait_terminal(job["job_id"])
        self.assertEqual(row["status"], "failed")
        self.assertEqual(row["termination_reason"], "hook_failure")
        payload = SERVER.get_job_result({"job_id": job["job_id"]})
        self.assertTrue(payload["partial"])

    def test_cancel_stops_running_job(self):
        job = SERVER.submit_job(
            {
                "task": "SCENARIO=timeout",
                "cwd": str(self.repo),
                "mode": "worktree",
                "timeout_seconds": 5,
            }
        )
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if SERVER.job_row(job["job_id"])["status"] == "running":
                break
            time.sleep(0.05)
        SERVER.cancel_job({"job_id": job["job_id"]})
        row = self.wait_terminal(job["job_id"])
        self.assertEqual(row["status"], "cancelled")
        events = SERVER.get_job_events({"job_id": job["job_id"], "after_seq": 0})
        self.assertIn("cancel_requested", [event["type"] for event in events["events"]])

    def test_legacy_wrapper_remains_compatible(self):
        payload = SERVER.legacy_delegate(
            {
                "task": "SCENARIO=success",
                "cwd": str(self.repo),
                "mode": "worktree",
                "timeout_seconds": 5,
            }
        )
        self.assertEqual(payload["status"], "completed_unverified")
        self.assertEqual(payload["result"]["summary"], "fake worker completed")

    def test_transient_failure_retries_with_fallback_model(self):
        job = SERVER.submit_job(
            {
                "task": "SCENARIO=model_fallback",
                "cwd": str(self.repo),
                "mode": "analyze",
                "timeout_seconds": 5,
                "model": "primary-model",
                "fallback_models": ["backup-model"],
                "max_attempts": 2,
            }
        )
        row = self.wait_terminal(job["job_id"])
        self.assertEqual(row["status"], "completed_unverified")
        self.assertEqual(row["attempt_count"], 2)
        self.assertEqual(row["selected_model"], "backup-model")
        self.assertGreater(row["cumulative_worker_tokens"], 15)
        events = SERVER.get_job_events({"job_id": job["job_id"], "after_seq": 0})
        self.assertIn("retry_scheduled", [event["type"] for event in events["events"]])

    def test_mcp_tasks_extension_wraps_delegate_submit_when_client_declares_support(self):
        meta = {
            "io.modelcontextprotocol/clientCapabilities": {
                "extensions": {"io.modelcontextprotocol/tasks": {}}
            }
        }
        response = SERVER.handle_request(
            {
                "jsonrpc": "2.0",
                "id": 11,
                "method": "tools/call",
                "params": {
                    "name": "delegate_submit",
                    "arguments": {
                        "task": "SCENARIO=success task-extension",
                        "cwd": str(self.repo),
                        "mode": "analyze",
                        "timeout_seconds": 5,
                    },
                    "_meta": meta,
                },
            }
        )
        handle = response["result"]
        self.assertEqual(handle["resultType"], "task")
        task_id = handle["taskId"]
        self.wait_terminal(task_id)
        polled = SERVER.handle_request(
            {
                "jsonrpc": "2.0",
                "id": 12,
                "method": "tasks/get",
                "params": {"taskId": task_id, "_meta": meta},
            }
        )
        self.assertEqual(polled["result"]["resultType"], "complete")
        self.assertEqual(polled["result"]["status"], "completed")
        self.assertFalse(polled["result"]["result"]["isError"])

    def test_research_tree_supports_parallel_and_recursive_branches(self):
        research = SERVER.create_research(
            {
                "objective": "Investigate three independent areas",
                "cwd": str(self.repo),
                "mode": "analyze",
                "result_detail": "standard",
                "max_depth": 3,
            }
        )
        batch = SERVER.add_research_branches(
            {
                "research_id": research["research_id"],
                "tasks": [
                    "SCENARIO=success SLEEP=0.6 branch=one",
                    "SCENARIO=success SLEEP=0.6 branch=two",
                    "SCENARIO=success SLEEP=0.6 branch=three",
                ],
                "timeout_seconds": 5,
            }
        )
        self.assertEqual(batch["submitted"], 3)
        deadline = time.monotonic() + 3
        max_running = 0
        while time.monotonic() < deadline:
            connection = SERVER.db_connect()
            try:
                running = connection.execute(
                    "SELECT COUNT(*) AS count FROM jobs WHERE status='running'"
                ).fetchone()["count"]
            finally:
                connection.close()
            max_running = max(max_running, int(running))
            if max_running >= 3:
                break
            time.sleep(0.03)
        self.assertEqual(max_running, 3)
        for branch in batch["branches"]:
            self.wait_terminal(branch["job_id"])

        child_batch = SERVER.add_research_branches(
            {
                "research_id": research["research_id"],
                "parent_branch_id": batch["branches"][0]["branch_id"],
                "tasks": ["SCENARIO=success child=one"],
                "timeout_seconds": 5,
            }
        )
        self.assertEqual(child_batch["branches"][0]["depth"], 1)
        self.wait_terminal(child_batch["branches"][0]["job_id"])

        snapshot = SERVER.research_snapshot({"research_id": research["research_id"]})
        self.assertEqual(snapshot["branch_count"], 4)
        self.assertTrue(snapshot["terminal"])
        self.assertEqual(
            next(item for item in snapshot["branches"] if item["depth"] == 1)["parent_branch_id"],
            batch["branches"][0]["branch_id"],
        )

    def test_research_packet_is_compact_unverified_and_aggregates_savings(self):
        research = SERVER.create_research(
            {"objective": "Compact packet", "cwd": str(self.repo), "mode": "analyze"}
        )
        batch = SERVER.add_research_branches(
            {
                "research_id": research["research_id"],
                "tasks": ["SCENARIO=success packet=one", "SCENARIO=success packet=two"],
                "timeout_seconds": 5,
            }
        )
        for branch in batch["branches"]:
            self.wait_terminal(branch["job_id"])
        packet = SERVER.research_packet({"research_id": research["research_id"]})
        self.assertEqual(packet["branch_count"], 2)
        self.assertEqual(packet["status"], "completed")
        self.assertTrue(all(item["verification_status"] == "unverified" for item in packet["branches"]))
        self.assertTrue(all(item["cline"]["requested_thinking"] == "high" for item in packet["branches"]))
        self.assertTrue(all(item["result"]["summary"] == "fake worker completed" for item in packet["branches"]))
        self.assertGreater(packet["metrics"]["raw_transcript_tokens_est"], 0)
        expected_avoided = 0
        for branch in batch["branches"]:
            expected_avoided += SERVER.get_job_result({"job_id": branch["job_id"]})["metrics"]["context_tokens_avoided_est"]
        self.assertEqual(packet["metrics"]["context_tokens_avoided_est"], expected_avoided)
        rendered = json.dumps(packet)
        self.assertNotIn("raw_contents", rendered)
        self.assertNotIn("cline.ndjson", rendered)

    def test_research_cancel_cascades_to_descendants(self):
        research = SERVER.create_research(
            {"objective": "Cancel all", "cwd": str(self.repo), "mode": "analyze"}
        )
        batch = SERVER.add_research_branches(
            {
                "research_id": research["research_id"],
                "tasks": ["SCENARIO=timeout cancel=one", "SCENARIO=timeout cancel=two"],
                "timeout_seconds": 5,
            }
        )
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            statuses = [SERVER.job_row(item["job_id"])["status"] for item in batch["branches"]]
            if any(status == "running" for status in statuses):
                break
            time.sleep(0.05)
        cancelled = SERVER.cancel_research({"research_id": research["research_id"]})
        self.assertEqual(cancelled["status"], "cancelled")
        rows = [self.wait_terminal(item["job_id"]) for item in batch["branches"]]
        self.assertTrue(all(row["status"] == "cancelled" for row in rows))

    def test_orchestration_api_wraps_legacy_research_storage(self):
        orchestration = SERVER.orchestration_create(
            {
                "objective": "Deep codebase analysis",
                "cwd": str(self.repo),
                "mode": "analyze",
                "max_depth": 2,
            }
        )
        self.assertIn("orchestration_id", orchestration)
        self.assertNotIn("research_id", orchestration)
        batch = SERVER.orchestration_add_branches(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "tasks": ["SCENARIO=success inspect architecture"],
                "timeout_seconds": 5,
            }
        )
        self.assertEqual(batch["orchestration_id"], orchestration["orchestration_id"])
        self.wait_terminal(batch["branches"][0]["job_id"])
        packet = SERVER.orchestration_packet(
            {"orchestration_id": orchestration["orchestration_id"]}
        )
        self.assertEqual(packet["status"], "completed")
        self.assertEqual(packet["branch_count"], 1)
        self.assertNotIn("research_id", packet)

    def test_orchestration_trace_links_codex_context_and_records_round_trip(self):
        orchestration = SERVER.orchestration_create(
            {
                "objective": "Trace observability",
                "cwd": str(self.repo),
                "mode": "analyze",
                "trace_context": {
                    "codex_thread_id": "thread-test-123",
                    "codex_turn_id": "turn-test-456",
                },
            }
        )
        batch = SERVER.orchestration_add_branches(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "tasks": [
                    "SCENARIO=success SLEEP=0.2 inspect A",
                    "SCENARIO=success SLEEP=0.2 inspect B",
                ],
                "timeout_seconds": 5,
                "estimated_worker_tokens": [1000, 1000],
            }
        )
        for branch in batch["branches"]:
            self.wait_terminal(branch["job_id"])
        SERVER.orchestration_packet({"orchestration_id": orchestration["orchestration_id"]})
        trace = SERVER.trace_get({"orchestration_id": orchestration["orchestration_id"]})
        self.assertEqual(trace["codex_thread_id"], "thread-test-123")
        self.assertEqual(trace["codex_turn_id"], "turn-test-456")
        self.assertEqual(len(trace["nodes"]), 2)
        self.assertGreaterEqual(trace["metrics"]["max_parallel_workers"], 1)
        event_types = {event["type"] for event in trace["timeline"]}
        self.assertIn("orchestration_created", event_types)
        self.assertIn("branches_added", event_types)
        self.assertIn("packet_returned_to_codex", event_types)
        self.assertIn("terminal", event_types)
        listing = SERVER.trace_list(
            {"last_n": 10, "codex_thread_id": "thread-test-123"}
        )
        self.assertEqual(len(listing["traces"]), 1)
        self.assertEqual(
            listing["traces"][0]["orchestration_id"],
            orchestration["orchestration_id"],
        )

    def test_orchestration_idempotency_reuses_branch(self):
        orchestration = SERVER.orchestration_create(
            {"objective": "Deduplicate", "cwd": str(self.repo), "mode": "analyze"}
        )
        first = SERVER.orchestration_add_branches(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "tasks": ["SCENARIO=success inspect same module"],
                "timeout_seconds": 5,
            }
        )
        second = SERVER.orchestration_add_branches(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "tasks": ["SCENARIO=success inspect same module"],
                "timeout_seconds": 5,
            }
        )
        self.assertEqual(first["submitted"], 1)
        self.assertEqual(second["submitted"], 0)
        self.assertEqual(second["reused"], 1)
        self.assertEqual(first["branches"][0]["branch_id"], second["branches"][0]["branch_id"])
        self.wait_terminal(first["branches"][0]["job_id"])

    def test_auto_budget_is_derived_from_per_branch_size_classes(self):
        orchestration = SERVER.orchestration_create(
            {"objective": "Auto budget", "cwd": str(self.repo), "mode": "analyze"}
        )
        self.assertEqual(orchestration["budgets"]["worker_budget_mode"], "auto")
        batch = SERVER.orchestration_add_branches(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "tasks": [
                    "SCENARIO=success narrow lookup",
                    "SCENARIO=success normal investigation",
                    "SCENARIO=success broad synthesis",
                ],
                "branch_result_details": ["compact", "standard", "research"],
                "timeout_seconds": 5,
            }
        )
        self.assertEqual(
            [item["estimated_worker_tokens"] for item in batch["branches"]],
            [300000, 600000, 1000000],
        )
        self.assertEqual(batch["budget"]["worker_budget_mode"], "auto")
        self.assertEqual(batch["budget"]["worker_tokens_reserved"], 1900000)
        self.assertEqual(batch["budget"]["worker_tokens_limit"], 2280000)
        for item in batch["branches"]:
            self.wait_terminal(item["job_id"])

    def test_orchestration_plan_mixed_classes_budget_timeouts_and_waves(self):
        plan = SERVER.orchestration_plan(
            {
                "tasks": ["narrow lookup", "normal investigation", "broad synthesis"],
                "branch_result_details": ["compact", "standard", "research"],
                "dependency_indices": [[], [0], [0]],
            }
        )
        self.assertEqual(
            [branch["reserved_worker_tokens"] for branch in plan["branches"]],
            [300000, 600000, 1000000],
        )
        self.assertEqual(
            [branch["timeout_seconds"] for branch in plan["branches"]],
            [900, 1800, 3600],
        )
        self.assertEqual(plan["reservation_total"], 1900000)
        self.assertEqual(plan["recommended_worker_token_budget"], 2280000)
        self.assertEqual(plan["waves"], [[0], [1, 2]])
        self.assertEqual([branch["wave"] for branch in plan["branches"]], [0, 1, 1])
        self.assertEqual(plan["estimated_dependency_critical_path_seconds"], 4500.0)

    def test_orchestration_plan_rejects_dependency_cycle(self):
        with self.assertRaisesRegex(ValueError, "cycle"):
            SERVER.orchestration_plan(
                {
                    "tasks": ["A", "B"],
                    "dependency_indices": [[1], [0]],
                }
            )

    def test_branch_default_timeouts_and_per_branch_override(self):
        orchestration = SERVER.orchestration_create(
            {"objective": "Timeout classes", "cwd": str(self.repo), "mode": "analyze"}
        )
        batch = SERVER.orchestration_add_branches(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "tasks": [
                    "SCENARIO=success compact timeout",
                    "SCENARIO=success standard timeout",
                    "SCENARIO=success research timeout",
                ],
                "branch_result_details": ["compact", "standard", "research"],
                "branch_timeout_seconds": [30, 45, 60],
            }
        )
        self.assertEqual([item["timeout_seconds"] for item in batch["branches"]], [30, 45, 60])
        for item in batch["branches"]:
            self.wait_terminal(item["job_id"])

        second = SERVER.orchestration_create(
            {"objective": "Timeout defaults", "cwd": str(self.repo), "mode": "analyze"}
        )
        defaults = SERVER.orchestration_add_branches(
            {
                "orchestration_id": second["orchestration_id"],
                "tasks": [
                    "SCENARIO=success compact default",
                    "SCENARIO=success standard default",
                    "SCENARIO=success research default",
                ],
                "branch_result_details": ["compact", "standard", "research"],
            }
        )
        self.assertEqual(
            [item["timeout_seconds"] for item in defaults["branches"]],
            [900, 1800, 3600],
        )
        for item in defaults["branches"]:
            self.wait_terminal(item["job_id"])

    def test_orchestration_escalates_terminal_branch_and_preserves_lineage(self):
        orchestration = SERVER.orchestration_create(
            {"objective": "Escalate", "cwd": str(self.repo), "mode": "analyze"}
        )
        batch = SERVER.orchestration_add_branches(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "tasks": ["SCENARIO=success compact first pass"],
                "branch_result_details": ["compact"],
                "timeout_seconds": 30,
            }
        )
        source = batch["branches"][0]
        self.wait_terminal(source["job_id"])
        escalated = SERVER.orchestration_escalate_branch(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "branch_id": source["branch_id"],
                "reason": "Need more evidence",
            }
        )
        child = escalated["branches"][0]
        self.assertEqual(escalated["from_result_detail"], "compact")
        self.assertEqual(escalated["to_result_detail"], "standard")
        self.assertEqual(child["parent_branch_id"], source["branch_id"])
        self.assertEqual(child["estimated_worker_tokens"], 600000)
        self.assertEqual(child["timeout_seconds"], 1800)
        self.wait_terminal(child["job_id"])

        trace = SERVER.trace_get({"orchestration_id": orchestration["orchestration_id"]})
        self.assertIn(source["branch_id"], trace["critical_path"]["branch_ids"])
        self.assertIn(child["branch_id"], trace["critical_path"]["branch_ids"])
        self.assertGreaterEqual(trace["budget"]["branch_reservation_total"], 900000)
        self.assertIn("observed_worker_tokens_total", trace["budget"])
        self.assertTrue(any(event["type"] == "branch_escalated" for event in trace["timeline"]))

    def test_research_branch_cannot_escalate(self):
        orchestration = SERVER.orchestration_create(
            {"objective": "No escalation", "cwd": str(self.repo), "mode": "analyze"}
        )
        batch = SERVER.orchestration_add_branches(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "tasks": ["SCENARIO=success already broad"],
                "branch_result_details": ["research"],
                "timeout_seconds": 30,
            }
        )
        branch = batch["branches"][0]
        self.wait_terminal(branch["job_id"])
        with self.assertRaisesRegex(ValueError, "highest"):
            SERVER.orchestration_escalate_branch(
                {
                    "orchestration_id": orchestration["orchestration_id"],
                    "branch_id": branch["branch_id"],
                }
            )

    def test_auto_budget_respects_ten_million_hard_cap(self):
        orchestration = SERVER.orchestration_create(
            {"objective": "Auto hard cap", "cwd": str(self.repo), "mode": "analyze"}
        )
        with self.assertRaisesRegex(ValueError, "hard cap"):
            SERVER.orchestration_add_branches(
                {
                    "orchestration_id": orchestration["orchestration_id"],
                    "tasks": [f"SCENARIO=success research {index}" for index in range(9)],
                    "branch_result_details": ["research"] * 9,
                    "timeout_seconds": 5,
                }
            )

    def test_orchestration_validates_full_batch_before_creating_jobs(self):
        orchestration = SERVER.orchestration_create(
            {"objective": "Atomic validation", "cwd": str(self.repo), "mode": "analyze"}
        )
        connection = SERVER.db_connect()
        try:
            before = connection.execute("SELECT COUNT(*) AS count FROM jobs").fetchone()["count"]
        finally:
            connection.close()
        with self.assertRaises(ValueError):
            SERVER.orchestration_add_branches(
                {
                    "orchestration_id": orchestration["orchestration_id"],
                    "tasks": ["SCENARIO=success valid", ""],
                    "timeout_seconds": 5,
                }
            )
        connection = SERVER.db_connect()
        try:
            after = connection.execute("SELECT COUNT(*) AS count FROM jobs").fetchone()["count"]
        finally:
            connection.close()
        self.assertEqual(before, after)

    def test_orchestration_branch_budget_is_enforced(self):
        orchestration = SERVER.orchestration_create(
            {
                "objective": "Budget",
                "cwd": str(self.repo),
                "mode": "analyze",
                "max_branches": 1,
            }
        )
        first = SERVER.orchestration_add_branches(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "tasks": ["SCENARIO=success one"],
                "timeout_seconds": 5,
            }
        )
        with self.assertRaises(ValueError):
            SERVER.orchestration_add_branches(
                {
                    "orchestration_id": orchestration["orchestration_id"],
                    "tasks": ["SCENARIO=success two"],
                    "timeout_seconds": 5,
                }
            )
        self.wait_terminal(first["branches"][0]["job_id"])

    def test_worker_token_reservation_prevents_parallel_budget_overshoot(self):
        orchestration = SERVER.orchestration_create(
            {
                "objective": "Token reservation",
                "cwd": str(self.repo),
                "mode": "analyze",
                "max_total_worker_tokens": 200000,
            }
        )
        connection = SERVER.db_connect()
        try:
            before = connection.execute("SELECT COUNT(*) AS count FROM jobs").fetchone()["count"]
        finally:
            connection.close()
        with self.assertRaises(ValueError):
            SERVER.orchestration_add_branches(
                {
                    "orchestration_id": orchestration["orchestration_id"],
                    "tasks": [
                        "SCENARIO=success compact one",
                        "SCENARIO=success compact two",
                    ],
                    "timeout_seconds": 5,
                }
            )
        connection = SERVER.db_connect()
        try:
            after = connection.execute("SELECT COUNT(*) AS count FROM jobs").fetchone()["count"]
        finally:
            connection.close()
        self.assertEqual(before, after)

    def test_dag_dependency_stays_blocked_until_parent_finishes(self):
        orchestration = SERVER.orchestration_create(
            {"objective": "DAG", "cwd": str(self.repo), "mode": "analyze"}
        )
        first = SERVER.orchestration_add_branches(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "tasks": ["SCENARIO=success SLEEP=0.4 parent"],
                "timeout_seconds": 5,
            }
        )
        parent_branch = first["branches"][0]
        child = SERVER.orchestration_add_branches(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "tasks": ["SCENARIO=success child"],
                "dependencies": [[parent_branch["branch_id"]]],
                "timeout_seconds": 5,
            }
        )
        child_job = child["branches"][0]["job_id"]
        self.assertEqual(SERVER.job_row(child_job)["status"], "blocked")
        self.wait_terminal(parent_branch["job_id"])
        self.wait_terminal(child_job)

    def test_dependency_failure_propagates_transitively(self):
        orchestration = SERVER.orchestration_create(
            {"objective": "Failure propagation", "cwd": str(self.repo), "mode": "analyze"}
        )
        batch = SERVER.orchestration_add_branches(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "tasks": [
                    "SCENARIO=hook_failure root",
                    "SCENARIO=success child",
                    "SCENARIO=success grandchild",
                ],
                "dependency_indices": [[], [0], [1]],
                "branch_timeout_seconds": [30, 30, 30],
                "use_semantic_cache": False,
            }
        )
        root, child, grandchild = batch["branches"]
        self.assertEqual(self.wait_terminal(root["job_id"])["status"], "failed")
        child_row = self.wait_terminal(child["job_id"])
        grandchild_row = self.wait_terminal(grandchild["job_id"])
        self.assertEqual(child_row["status"], "failed")
        self.assertEqual(child_row["termination_reason"], "dependency_failed")
        self.assertEqual(grandchild_row["status"], "failed")
        self.assertEqual(grandchild_row["termination_reason"], "dependency_failed")

    def test_hard_token_budget_prevents_releasing_dependent_branch(self):
        orchestration = SERVER.orchestration_create(
            {
                "objective": "Hard budget DAG",
                "cwd": str(self.repo),
                "mode": "analyze",
                "max_total_worker_tokens": 600000,
            }
        )
        first = SERVER.orchestration_add_branches(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "tasks": ["SCENARIO=success SLEEP=0.2 parent"],
                "timeout_seconds": 5,
            }
        )
        parent_branch = first["branches"][0]
        child = SERVER.orchestration_add_branches(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "tasks": ["SCENARIO=success child"],
                "dependencies": [[parent_branch["branch_id"]]],
                "timeout_seconds": 5,
            }
        )
        child_job = child["branches"][0]["job_id"]
        self.assertEqual(SERVER.job_row(child_job)["status"], "blocked")
        connection = SERVER.db_connect()
        try:
            connection.execute(
                "UPDATE researches SET max_total_worker_tokens=10 WHERE research_id=?",
                (orchestration["orchestration_id"],),
            )
        finally:
            connection.close()
        self.wait_terminal(parent_branch["job_id"])
        child_row = self.wait_terminal(child_job)
        self.assertEqual(child_row["status"], "cancelled")
        self.assertEqual(
            SERVER.research_row(orchestration["orchestration_id"])["status"],
            "budget_exhausted",
        )

    def test_overlap_warnings_flag_near_duplicate_branches(self):
        warnings = SERVER.branch_overlap_warnings(
            [
                "analyze payment module architecture dependencies and data flow",
                "analyze payment module architecture dependencies and error flow",
            ]
        )
        self.assertTrue(warnings)

    def test_claim_level_verification_is_persisted(self):
        orchestration = SERVER.orchestration_create(
            {"objective": "Verify", "cwd": str(self.repo), "mode": "analyze"}
        )
        batch = SERVER.orchestration_add_branches(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "tasks": ["SCENARIO=success verify"],
                "timeout_seconds": 5,
            }
        )
        branch = batch["branches"][0]
        self.wait_terminal(branch["job_id"])
        result = SERVER.orchestration_record_verification(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "branch_id": branch["branch_id"],
                "claims": [
                    {
                        "claim_id": "claim-1",
                        "status": "verified",
                        "evidence": ["independent-check:1"],
                    }
                ],
            }
        )
        self.assertEqual(result["verification_status"], "verified")
        packet = SERVER.orchestration_packet(
            {"orchestration_id": orchestration["orchestration_id"]}
        )
        self.assertEqual(packet["verification_metrics"]["claims_verified"], 1)
        self.assertEqual(packet["branches"][0]["verification_status"], "verified")

    def test_verified_result_is_reused_from_semantic_cache(self):
        first = SERVER.orchestration_create(
            {"objective": "Populate cache", "cwd": str(self.repo), "mode": "analyze"}
        )
        first_batch = SERVER.orchestration_add_branches(
            {
                "orchestration_id": first["orchestration_id"],
                "tasks": ["SCENARIO=success semantic cache target"],
                "timeout_seconds": 5,
            }
        )
        source = first_batch["branches"][0]
        self.wait_terminal(source["job_id"])
        verified = SERVER.orchestration_record_verification(
            {
                "orchestration_id": first["orchestration_id"],
                "branch_id": source["branch_id"],
                "claims": [
                    {"claim_id": "claim-1", "status": "verified", "evidence": ["manual-check:1"]}
                ],
            }
        )
        self.assertTrue(verified["semantic_cache_id"])

        second = SERVER.orchestration_create(
            {"objective": "Reuse cache", "cwd": str(self.repo), "mode": "analyze"}
        )
        reused = SERVER.orchestration_add_branches(
            {
                "orchestration_id": second["orchestration_id"],
                "tasks": ["SCENARIO=success semantic cache target"],
                "timeout_seconds": 5,
            }
        )
        branch = reused["branches"][0]
        row = SERVER.job_row(branch["job_id"])
        self.assertEqual(reused["semantic_cache_hits"], 1)
        self.assertTrue(branch["cache_hit"])
        self.assertEqual(row["status"], "verified")
        self.assertEqual(row["termination_reason"], "semantic_cache_hit")
        self.assertEqual(branch["estimated_worker_tokens"], 0)

    def test_semantic_cache_is_invalidated_when_repository_changes(self):
        first = SERVER.orchestration_create(
            {"objective": "Populate cache", "cwd": str(self.repo), "mode": "analyze"}
        )
        first_batch = SERVER.orchestration_add_branches(
            {
                "orchestration_id": first["orchestration_id"],
                "tasks": ["SCENARIO=success repository-sensitive target"],
                "timeout_seconds": 5,
            }
        )
        source = first_batch["branches"][0]
        self.wait_terminal(source["job_id"])
        verified = SERVER.orchestration_record_verification(
            {
                "orchestration_id": first["orchestration_id"],
                "branch_id": source["branch_id"],
                "claims": [
                    {"claim_id": "claim-1", "status": "verified", "evidence": ["manual-check:1"]}
                ],
            }
        )
        self.assertTrue(verified["semantic_cache_id"])

        (self.repo / "tracked.txt").write_text("changed\n", encoding="utf-8")
        second = SERVER.orchestration_create(
            {"objective": "Do not reuse stale cache", "cwd": str(self.repo), "mode": "analyze"}
        )
        batch = SERVER.orchestration_add_branches(
            {
                "orchestration_id": second["orchestration_id"],
                "tasks": ["SCENARIO=success repository-sensitive target"],
                "timeout_seconds": 5,
            }
        )
        branch = batch["branches"][0]
        self.assertEqual(batch["semantic_cache_hits"], 0)
        self.assertFalse(branch["cache_hit"])
        self.wait_terminal(branch["job_id"])

    def test_auto_verify_checks_file_line_evidence_without_worker(self):
        proof = self.repo / "proof.txt"
        proof.write_text("feature flag enabled true\n", encoding="utf-8")
        orchestration = SERVER.orchestration_create(
            {"objective": "Auto verify", "cwd": str(self.repo), "mode": "analyze"}
        )
        batch = SERVER.orchestration_add_branches(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "tasks": ["SCENARIO=success auto verify"],
                "timeout_seconds": 5,
                "use_semantic_cache": False,
            }
        )
        branch = batch["branches"][0]
        self.wait_terminal(branch["job_id"])
        row = SERVER.job_row(branch["job_id"])
        payload = json.loads(row["result_json"])
        payload["result"] = SERVER.normalize_result(
            {
                "summary": "checked",
                "claims": [
                    {
                        "claim_id": "claim-1",
                        "statement": "feature flag enabled true",
                        "evidence": ["proof.txt:1"],
                        "confidence": 0.9,
                        "importance": "high",
                    }
                ],
            }
        )
        connection = SERVER.db_connect()
        try:
            connection.execute(
                "UPDATE jobs SET result_json=?,verification_status='unverified' WHERE job_id=?",
                (SERVER.compact_json(payload), branch["job_id"]),
            )
        finally:
            connection.close()
        result = SERVER.orchestration_auto_verify(
            {"orchestration_id": orchestration["orchestration_id"], "spawn_verifiers": False}
        )
        self.assertEqual(result["recorded_branches"], 1)
        packet = SERVER.orchestration_packet(
            {"orchestration_id": orchestration["orchestration_id"]}
        )
        self.assertEqual(packet["branches"][0]["verification_status"], "evidence_validated")
        self.assertEqual(packet["verification_metrics"]["claims_verified"], 0)
        self.assertEqual(packet["verification_metrics"]["claims_evidence_validated"], 1)

    def test_high_importance_claim_requests_independent_verifier(self):
        proof = self.repo / "proof.txt"
        proof.write_text("feature flag enabled true\n", encoding="utf-8")
        orchestration = SERVER.orchestration_create(
            {"objective": "Verify important claim", "cwd": str(self.repo), "mode": "analyze"}
        )
        batch = SERVER.orchestration_add_branches(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "tasks": ["SCENARIO=success important claim"],
                "timeout_seconds": 5,
                "use_semantic_cache": False,
            }
        )
        branch = batch["branches"][0]
        self.wait_terminal(branch["job_id"])
        row = SERVER.job_row(branch["job_id"])
        payload = json.loads(row["result_json"])
        payload["result"] = SERVER.normalize_result(
            {
                "summary": "checked",
                "claims": [
                    {
                        "claim_id": "claim-1",
                        "statement": "feature flag enabled true",
                        "evidence": ["proof.txt:1"],
                        "confidence": 0.99,
                        "importance": "critical",
                    }
                ],
            }
        )
        connection = SERVER.db_connect()
        try:
            connection.execute(
                "UPDATE jobs SET result_json=?,verification_status='unverified' WHERE job_id=?",
                (SERVER.compact_json(payload), branch["job_id"]),
            )
        finally:
            connection.close()
        result = SERVER.orchestration_auto_verify(
            {"orchestration_id": orchestration["orchestration_id"], "spawn_verifiers": True}
        )
        self.assertTrue(result["waiting_for_verifiers"])
        self.assertEqual(len(result["spawned_verifiers"]), 1)
        packet = SERVER.orchestration_packet(
            {"orchestration_id": orchestration["orchestration_id"]}
        )
        source = next(item for item in packet["branches"] if item["branch_id"] == branch["branch_id"])
        self.assertEqual(source["verification_status"], "evidence_validated")
        self.assertNotEqual(source["verification_status"], "verified")
        self.wait_terminal(result["spawned_verifiers"][0]["job_id"])

    def test_replan_derives_next_wave_from_open_question(self):
        orchestration = SERVER.orchestration_create(
            {
                "objective": "Dynamic DAG",
                "cwd": str(self.repo),
                "mode": "analyze",
                "max_replan_rounds": 2,
            }
        )
        batch = SERVER.orchestration_add_branches(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "tasks": ["SCENARIO=success discover question"],
                "timeout_seconds": 5,
                "use_semantic_cache": False,
            }
        )
        branch = batch["branches"][0]
        self.wait_terminal(branch["job_id"])
        row = SERVER.job_row(branch["job_id"])
        payload = json.loads(row["result_json"])
        payload["result"] = SERVER.normalize_result(
            {
                "summary": "needs follow-up",
                "claims": [],
                "open_questions": ["Where is the fallback configuration loaded?"],
            }
        )
        connection = SERVER.db_connect()
        try:
            connection.execute(
                "UPDATE jobs SET result_json=? WHERE job_id=?",
                (SERVER.compact_json(payload), branch["job_id"]),
            )
        finally:
            connection.close()
        planned = SERVER.orchestration_replan(
            {"orchestration_id": orchestration["orchestration_id"], "apply": False}
        )
        self.assertEqual(planned["action"], "plan")
        self.assertEqual(planned["round"], 1)
        self.assertEqual(planned["candidates"][0]["reason"], "open_question")
        launched = SERVER.orchestration_replan(
            {"orchestration_id": orchestration["orchestration_id"], "apply": True}
        )
        self.assertEqual(launched["action"], "launched")
        self.assertEqual(launched["round"], 1)
        self.assertEqual(len(launched["branches"]), 1)
        self.wait_terminal(launched["branches"][0]["job_id"])

    def test_plan_output_executes_as_one_in_batch_dag(self):
        orchestration = SERVER.orchestration_create(
            {"objective": "Executable plan", "cwd": str(self.repo), "mode": "analyze"}
        )
        plan = SERVER.orchestration_plan(
            {
                "tasks": [
                    "SCENARIO=success SLEEP=0.15 root",
                    "SCENARIO=success child a",
                    "SCENARIO=success child b",
                ],
                "branch_result_details": ["compact", "compact", "compact"],
                "branch_timeout_seconds": [30, 30, 30],
                "dependency_indices": [[], [0], [0]],
            }
        )
        batch = SERVER.orchestration_add_branches(
            {
                "orchestration_id": orchestration["orchestration_id"],
                **plan["add_branches_arguments"],
            }
        )
        branches = batch["branches"]
        root_id = branches[0]["branch_id"]
        self.assertEqual(branches[1]["depends_on"], [root_id])
        self.assertEqual(branches[2]["depends_on"], [root_id])
        self.assertEqual(SERVER.job_row(branches[1]["job_id"])["status"], "blocked")
        for branch in branches:
            self.wait_terminal(branch["job_id"])

    def test_critical_path_handles_dependency_on_later_batch_index(self):
        orchestration = SERVER.orchestration_create(
            {"objective": "Topological trace", "cwd": str(self.repo), "mode": "analyze"}
        )
        batch = SERVER.orchestration_add_branches(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "tasks": ["SCENARIO=success consumer", "SCENARIO=success producer"],
                "dependency_indices": [[1], []],
                "branch_timeout_seconds": [30, 30],
            }
        )
        for branch in batch["branches"]:
            self.wait_terminal(branch["job_id"])
        trace = SERVER.trace_get({"orchestration_id": orchestration["orchestration_id"]})
        self.assertEqual(
            trace["critical_path"]["branch_ids"],
            [batch["branches"][1]["branch_id"], batch["branches"][0]["branch_id"]],
        )

    def test_large_fanout_only_launches_supervisors_up_to_worker_capacity(self):
        orchestration = SERVER.orchestration_create(
            {"objective": "Admission control", "cwd": str(self.repo), "mode": "analyze"}
        )
        batch = SERVER.orchestration_add_branches(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "tasks": [f"SCENARIO=success SLEEP=0.25 branch {i}" for i in range(10)],
                "branch_timeout_seconds": [30] * 10,
            }
        )
        connection = SERVER.db_connect()
        try:
            rows = connection.execute(
                "SELECT j.status,j.supervisor_pid FROM research_branches b JOIN jobs j ON j.job_id=b.job_id "
                "WHERE b.research_id=?",
                (orchestration["orchestration_id"],),
            ).fetchall()
        finally:
            connection.close()
        launched = sum(1 for row in rows if row["supervisor_pid"])
        blocked = sum(1 for row in rows if row["status"] == "blocked")
        self.assertLessEqual(launched, 3)
        self.assertGreaterEqual(blocked, 7)
        for branch in batch["branches"]:
            self.wait_terminal(branch["job_id"], timeout=15)
        trace = SERVER.trace_get({"orchestration_id": orchestration["orchestration_id"]})
        self.assertLessEqual(trace["metrics"]["max_parallel_workers"], 3)

    def test_trace_marks_legacy_zero_reservation_as_unavailable_and_thinking_as_requested_only(self):
        orchestration = SERVER.orchestration_create(
            {"objective": "Legacy telemetry", "cwd": str(self.repo), "mode": "analyze"}
        )
        batch = SERVER.orchestration_add_branches(
            {
                "orchestration_id": orchestration["orchestration_id"],
                "tasks": ["SCENARIO=success old branch"],
                "branch_timeout_seconds": [30],
            }
        )
        branch = batch["branches"][0]
        self.wait_terminal(branch["job_id"])
        connection = SERVER.db_connect()
        try:
            connection.execute(
                "UPDATE research_branches SET estimated_worker_tokens=0 WHERE branch_id=?",
                (branch["branch_id"],),
            )
        finally:
            connection.close()
        trace = SERVER.trace_get({"orchestration_id": orchestration["orchestration_id"]})
        node = trace["nodes"][0]
        self.assertEqual(node["reservation_source"], "legacy_unavailable")
        self.assertEqual(node["reserved_worker_tokens"], 0)
        self.assertEqual(node["requested_thinking"], "high")
        self.assertIsNone(node["effective_thinking"])

    def test_prune_removes_old_raw_artifacts_but_keeps_compact_database_result(self):
        submitted = SERVER.submit_job(
            {"task": "SCENARIO=success prune", "cwd": str(self.repo), "timeout_seconds": 30}
        )
        row = self.wait_terminal(submitted["job_id"])
        run_dir = Path(row["run_dir"])
        self.assertTrue(run_dir.is_dir())
        old = (SERVER.dt.datetime.now(SERVER.dt.timezone.utc) - SERVER.dt.timedelta(days=10)).isoformat()
        connection = SERVER.db_connect()
        try:
            connection.execute(
                "UPDATE jobs SET finished_at=? WHERE job_id=?", (old, submitted["job_id"])
            )
        finally:
            connection.close()
        preview = SERVER.prune_state({"dry_run": True, "raw_retention_days": 1, "max_run_dirs": 500})
        self.assertGreaterEqual(preview["candidate_run_dirs"], 1)
        applied = SERVER.prune_state({"dry_run": False, "raw_retention_days": 1, "max_run_dirs": 500})
        self.assertGreaterEqual(applied["removed_run_dirs"], 1)
        self.assertFalse(run_dir.exists())
        result = SERVER.get_job_result({"job_id": submitted["job_id"]})
        self.assertEqual(result["status"], "completed_unverified")

    def test_default_allowed_root_is_portable_home_and_model_policy_is_explicit(self):
        os.environ.pop("CLINE_DELEGATOR_ALLOWED_ROOTS", None)
        self.assertEqual(SERVER.allowed_roots(), [Path.home().resolve()])
        os.environ["CLINE_DELEGATOR_PREFERRED_MODEL"] = "preferred-model"
        os.environ.pop("CLINE_DELEGATOR_MODEL", None)
        self.assertEqual(SERVER.preferred_model(), "preferred-model")
        os.environ["CLINE_DELEGATOR_MODEL"] = "override-model"
        self.assertEqual(SERVER.preferred_model(), "override-model")


if __name__ == "__main__":
    unittest.main()
