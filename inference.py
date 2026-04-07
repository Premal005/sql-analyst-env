"""
Inference Script — SQL Analyst OpenEnv
=======================================
Mandatory environment variables:
    API_BASE_URL     The API endpoint for the LLM
    MODEL_NAME       The model identifier to use
    HF_TOKEN         Your HuggingFace / API key
    IMAGE_NAME       Docker image name (used by judges to spin up environment)

Optional:
    SQL_ENV_TASK     Run a single task (default: all 4 tasks)
    SQL_ENV_URL      Server URL override (default: http://localhost:7860)

Stdout format (strictly per spec):
    [START] task=<task> env=<benchmark> model=<model>
    [STEP]  step=<n> action=<sql> reward=<0.00> done=<true|false> error=<msg|null>
    [END]   success=<true|false> steps=<n> rewards=<r1,r2,...>
"""

import asyncio
import json
import os
import subprocess
import sys
import textwrap
import time
import urllib.request
import urllib.error
from typing import List, Optional

from openai import OpenAI

# ── Config ────────────────────────────────────────────────────────────────────

IMAGE_NAME   = os.getenv("IMAGE_NAME", "")
API_KEY      = os.getenv("HF_TOKEN") or os.getenv("API_KEY", "")
API_BASE_URL = os.getenv("API_BASE_URL", "https://router.huggingface.co/v1")
MODEL_NAME   = os.getenv("MODEL_NAME", "Qwen/Qwen2.5-72B-Instruct")
BENCHMARK    = "sql-analyst"

ALL_TASKS    = ["simple_select", "aggregate_join", "window_function", "mom_growth"]
TASKS_TO_RUN = [os.getenv("SQL_ENV_TASK")] if os.getenv("SQL_ENV_TASK") else ALL_TASKS

MAX_STEPS_MAP = {
    "simple_select":   5,
    "aggregate_join":  6,
    "window_function": 8,
    "mom_growth":      10,
}

SUCCESS_THRESH = 0.80
BASE_URL = os.getenv("SQL_ENV_URL", "http://localhost:7860")


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _post(path: str, body: dict, timeout: int = 60) -> dict:
    url  = f"{BASE_URL}{path}"
    data = json.dumps(body).encode()
    req  = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def _get(path: str, timeout: int = 30) -> dict:
    with urllib.request.urlopen(f"{BASE_URL}{path}", timeout=timeout) as resp:
        return json.loads(resp.read())


# ── Docker helpers ────────────────────────────────────────────────────────────

def start_docker(image_name: str) -> Optional[str]:
    """Start the environment Docker container. Returns container_id or None."""
    global BASE_URL
    try:
        proc = subprocess.run(
            ["docker", "run", "-d", "--rm", "-p", "7860:7860", image_name],
            capture_output=True, text=True, timeout=60,
        )
        if proc.returncode != 0:
            print(f"[DEBUG] docker run failed: {proc.stderr.strip()}", flush=True)
            return None
        container_id = proc.stdout.strip()
        BASE_URL = "http://localhost:7860"
        # Wait for server to be ready (up to 60s)
        for i in range(60):
            time.sleep(1)
            try:
                _get("/health", timeout=5)
                print(f"[DEBUG] Container ready after {i+1}s", flush=True)
                return container_id
            except Exception:
                pass
        print("[DEBUG] Container did not become ready in 60s", flush=True)
        return container_id  # Return anyway, let episode handle failures
    except Exception as exc:
        print(f"[DEBUG] start_docker error: {exc}", flush=True)
        return None


def stop_docker(container_id: Optional[str]) -> None:
    if container_id:
        try:
            subprocess.run(
                ["docker", "stop", container_id],
                capture_output=True, timeout=30,
            )
        except Exception:
            pass


# ── Logging (exact spec format) ───────────────────────────────────────────────

def log_start(task: str, model: str) -> None:
    print(f"[START] task={task} env={BENCHMARK} model={model}", flush=True)


def log_step(step: int, action: str, reward: float, done: bool, error: Optional[str]) -> None:
    action_oneline = str(action).replace("\n", " ").replace("\r", "").strip()
    err_val = str(error).replace("\n", " ") if error else "null"
    print(
        f"[STEP] step={step} action={action_oneline!r} "
        f"reward={reward:.2f} done={str(done).lower()} error={err_val}",
        flush=True,
    )


def log_end(success: bool, steps: int, rewards: List[float]) -> None:
    rewards_str = ",".join(f"{r:.2f}" for r in rewards)
    print(
        f"[END] success={str(success).lower()} steps={steps} rewards={rewards_str}",
        flush=True,
    )


# ── LLM call ─────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = textwrap.dedent("""
    You are an expert SQL data analyst. Given a database schema and a
    natural-language business question, write a correct SQLite SQL query
    that answers the question exactly.

    Strict rules:
    - Output ONLY the raw SQL — no markdown, no backticks, no explanation.
    - Use standard SQLite syntax (CTEs with WITH, window functions supported).
    - Always alias aggregated columns with meaningful names (e.g. AS total_revenue).
    - If you receive a low score or an error, analyse it and fix the query.
""").strip()


def build_prompt(obs: dict) -> str:
    parts = [
        f"## Database Schema\n{obs.get('schema_description', '')}",
        f"## Business Question\n{obs.get('question', '')}",
    ]
    if obs.get("sample_data"):
        parts.append(f"## Sample Data\n{obs['sample_data']}")
    if obs.get("last_query"):
        parts.append(f"## Your Previous Query\n{obs['last_query']}")
        if obs.get("last_error"):
            parts.append(f"## SQL Error — fix this\n{obs['last_error']}")
        elif obs.get("last_result"):
            score = obs.get("last_score") or 0.0
            parts.append(f"## Result Preview (score={score:.2f})\n{obs['last_result']}")
    if obs.get("hint"):
        parts.append(f"## Hint\n{obs['hint']}")
    parts.append("## Write the correct SQL query now:")
    return "\n\n".join(parts)


def get_sql(client: OpenAI, obs: dict) -> str:
    try:
        completion = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": build_prompt(obs)},
            ],
            temperature=0.2,
            max_tokens=512,
            stream=False,
        )
        sql = (completion.choices[0].message.content or "").strip()
        # Strip accidental markdown fences
        if sql.startswith("```"):
            sql = "\n".join(
                line for line in sql.splitlines()
                if not line.startswith("```")
            ).strip()
        return sql or "SELECT 1"
    except Exception as exc:
        print(f"[DEBUG] Model call failed: {exc}", flush=True)
        return "SELECT 1"


# ── Episode runner ────────────────────────────────────────────────────────────

def run_episode(task_id: str, client: OpenAI) -> float:
    """Run one full episode. Always emits [START]...[END]. Returns score."""
    max_steps   = MAX_STEPS_MAP.get(task_id, 8)
    rewards:    List[float] = []
    steps_taken = 0
    score       = 0.0
    success     = False

    log_start(task=task_id, model=MODEL_NAME)

    try:
        # Reset with task_id
        reset_resp = _post("/reset", {"task_id": task_id}, timeout=30)
        obs = reset_resp.get("observation", {})

        for step in range(1, max_steps + 1):
            query  = get_sql(client, obs)
            result = _post("/step", {"query": query}, timeout=60)

            reward = float(result.get("reward", 0.0))
            done   = bool(result.get("done", False))
            obs    = result.get("observation", obs)
            error  = obs.get("last_error") if isinstance(obs, dict) else None

            rewards.append(reward)
            steps_taken = step
            log_step(step=step, action=query, reward=reward, done=done, error=error)

            if done:
                break

        score   = float(obs.get("last_score") or 0.0) if isinstance(obs, dict) else 0.0
        success = score >= SUCCESS_THRESH

    except Exception as exc:
        print(f"[DEBUG] Episode error: {exc}", flush=True)

    finally:
        # Always emit [END] — even on exception
        log_end(success=success, steps=steps_taken, rewards=rewards)

    return score


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    client = OpenAI(base_url=API_BASE_URL, api_key=API_KEY)
    container_id: Optional[str] = None

    try:
        if IMAGE_NAME:
            print(f"[DEBUG] Starting Docker container: {IMAGE_NAME}", flush=True)
            container_id = start_docker(IMAGE_NAME)

        scores = []
        for task_id in TASKS_TO_RUN:
            try:
                s = run_episode(task_id, client)
                scores.append(s)
                print(f"[INFO] Task '{task_id}' finished. Score: {s:.3f}", flush=True)
            except Exception as exc:
                print(f"[DEBUG] Task '{task_id}' crashed: {exc}", flush=True)
                scores.append(0.0)

        overall = sum(scores) / len(scores) if scores else 0.0
        print(f"[INFO] Overall score across {len(scores)} task(s): {overall:.3f}", flush=True)

    except Exception as exc:
        print(f"[DEBUG] Fatal error in main: {exc}", flush=True)

    finally:
        stop_docker(container_id)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exc:
        print(f"[DEBUG] Unhandled exception: {exc}", flush=True)
        sys.exit(1)
