
"""
FastAPI application for the SQL Analyst environment.

Uses simple HTTP endpoints (not WebSocket) so reset/step/state
share the same environment instance reliably.

openenv validate passes because the file structure, pyproject.toml
scripts entry, and openenv.yaml all comply with the spec.
"""

import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

try:
    from .sql_analyst_env_environment import SqlAnalystEnvironment
    from .tasks import TASKS
except ImportError:
    from server.sql_analyst_env_environment import SqlAnalystEnvironment
    from server.tasks import TASKS


# ── Global environment instance ───────────────────────────────────────────────

_env: Optional[SqlAnalystEnvironment] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _env
    _env = SqlAnalystEnvironment()
    _env.reset()
    yield
    if _env:
        try:
            if _env._conn:
                _env._conn.close()
        except Exception:
            pass


app = FastAPI(
    title="SQL Analyst OpenEnv",
    description="OpenEnv environment for evaluating Text-to-SQL agents.",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Request / Response models ─────────────────────────────────────────────────

class ResetRequest(BaseModel):
    task_id: Optional[str] = None


class StepRequest(BaseModel):
    query: str


class ConfigureRequest(BaseModel):
    task_id: str


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.post("/reset")
def reset(body: ResetRequest = None):
    """Reset the environment. Reads SQL_ENV_TASK env var (set via /configure)."""
    global _env
    if body and body.task_id:
        if body.task_id not in TASKS:
            raise HTTPException(400, f"Unknown task '{body.task_id}'. Valid: {list(TASKS)}")
        os.environ["SQL_ENV_TASK"] = body.task_id

    if _env is None:
        _env = SqlAnalystEnvironment()

    obs = _env.reset()
    return {
        "observation": obs.model_dump(),
        "reward": 0.001,
        "done": False,
    }


@app.post("/step")
def step(body: StepRequest):
    """Submit a SQL query and receive a scored observation."""
    if _env is None:
        raise HTTPException(400, "Call /reset first.")
    try:
        obs = _env.step_with_query(body.query)
        reward = round(max(0.001, min(0.998, float(obs.reward))), 4)
        return {
            "observation": obs.model_dump(),
            "reward": reward,
            "done": obs.done,
        }
    except Exception as exc:
        raise HTTPException(400, str(exc))


@app.get("/state")
def state():
    """Return current episode state."""
    if _env is None:
        raise HTTPException(400, "Call /reset first.")
    s = _env.state
    return {
        "episode_id": s.episode_id,
        "step_count": s.step_count,
        "task_id": _env._task.task_id if _env._task else None,
        "best_score": _env._best_score,
        "done": _env._done,
    }


@app.post("/configure")
def configure(body: ConfigureRequest):
    """Set the active task before calling /reset."""
    if body.task_id not in TASKS:
        raise HTTPException(400, f"Unknown task '{body.task_id}'. Valid: {list(TASKS)}")
    os.environ["SQL_ENV_TASK"] = body.task_id
    return {"configured": True, "task_id": body.task_id}


@app.get("/tasks")
def list_tasks():
    """List all available tasks."""
    return {
        tid: {
            "difficulty": t.difficulty,
            "max_steps": t.max_steps,
            "question": t.question,
        }
        for tid, t in TASKS.items()
    }


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "task": _env._task.task_id if (_env and _env._task) else None,
    }


# ── Entry point ───────────────────────────────────────────────────────────────

def main(host: str = "0.0.0.0", port: int = 7860):
    """Entry point: uv run server"""
    import uvicorn
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=7860)
    args = parser.parse_args()
    main(port=args.port)

if __name__ == '__main__':
    main()
