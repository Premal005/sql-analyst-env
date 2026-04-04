"""
SQL Analyst Environment — Core Implementation

An agent receives a database schema + a natural-language business question
and must write SQL queries to answer it correctly. Rewards partial progress
at every step so the agent can iteratively improve its query.
"""

import os
import sqlite3
from typing import Optional
from uuid import uuid4

from openenv.core.env_server.interfaces import Environment
from openenv.core.env_server.types import State

try:
    from ..models import SqlAnalystAction, SqlAnalystObservation
except ImportError:
    from models import SqlAnalystAction, SqlAnalystObservation

try:
    from .tasks import TASKS, Task, build_db
    from .grader import grade
except ImportError:
    from tasks import TASKS, Task, build_db
    from grader import grade


DONE_THRESHOLD = 0.95   # score >= this ends episode as success
STEP_DECAY     = 0.05   # reward multiplier decreases per step
DEFAULT_TASK   = os.getenv("SQL_ENV_TASK", "simple_select")


class SqlAnalystEnvironment(Environment):
    """
    SQL Analyst environment for evaluating Text-to-SQL agents.

    The agent receives a schema, sample data, and a business question.
    It submits SQL queries and receives partial-credit rewards based on
    how closely its result matches the reference answer.

    Supports 4 tasks of increasing difficulty:
      - simple_select   (easy)       SELECT + WHERE + ORDER BY
      - aggregate_join  (medium)     JOIN + GROUP BY + SUM
      - window_function (hard)       CTE + RANK() OVER PARTITION BY
      - mom_growth      (very_hard)  LAG() + STRFTIME + CASE WHEN
    """

    SUPPORTS_CONCURRENT_SESSIONS: bool = True

    def __init__(self):
        self._task: Task = TASKS[DEFAULT_TASK]
        self._conn: Optional[sqlite3.Connection] = None
        self._step_number: int = 0
        self._done: bool = False
        self._best_score: float = 0.0
        self._last_obs: Optional[SqlAnalystObservation] = None
        self._state = State(episode_id=str(uuid4()), step_count=0)

    # ── OpenEnv interface ────────────────────────────────────────────────────

    def reset(self) -> SqlAnalystObservation:
        """Reset episode: rebuild DB, clear state, return initial observation."""
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass

        self._conn       = build_db(self._task)
        self._step_number = 0
        self._done        = False
        self._best_score  = 0.0
        self._state       = State(episode_id=str(uuid4()), step_count=0)

        obs = SqlAnalystObservation(
            task_id=self._task.task_id,
            difficulty=self._task.difficulty,
            schema_description=self._task.schema_description,
            question=self._task.question,
            sample_data=self._get_sample(),
            step_number=0,
            max_steps=self._task.max_steps,
            done=False,
            reward=0.0,
        )
        self._last_obs = obs
        return obs

    def step(self, action: SqlAnalystAction) -> SqlAnalystObservation:
        """Execute the agent's SQL query and return scored observation."""
        if self._done:
            # Return last obs unchanged — episode already ended
            return self._last_obs

        self._step_number     += 1
        self._state.step_count += 1

        result  = grade(self._conn, action.query.strip(), self._task.expected_query)
        score   = result["score"]
        error   = result["error"]
        preview = result["result_preview"]

        # Reward: score × decay − error_penalty (floor 0)
        decay  = max(0.5, 1.0 - STEP_DECAY * (self._step_number - 1))
        reward = max(round(score * decay - (0.10 if error else 0.0), 4), 0.0)

        self._best_score = max(self._best_score, score)
        self._done = (score >= DONE_THRESHOLD) or (self._step_number >= self._task.max_steps)

        # Progressive hint after step 3 if still struggling
        hint: Optional[str] = None
        if self._step_number >= 3 and score < 0.5 and self._task.hints:
            idx  = min(self._step_number - 3, len(self._task.hints) - 1)
            hint = f"Hint {idx + 1}: {self._task.hints[idx]}"

        obs = SqlAnalystObservation(
            task_id=self._task.task_id,
            difficulty=self._task.difficulty,
            schema_description=self._task.schema_description,
            question=self._task.question,
            sample_data=self._last_obs.sample_data if self._last_obs else None,
            last_query=action.query.strip(),
            last_result=preview,
            last_error=error,
            last_score=score,
            step_number=self._step_number,
            max_steps=self._task.max_steps,
            hint=hint,
            done=self._done,
            reward=reward,
        )
        self._last_obs = obs
        return obs

    @property
    def state(self) -> State:
        return self._state

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _get_sample(self) -> Optional[str]:
        """Return a formatted sample of the task data for the agent."""
        try:
            cur  = self._conn.cursor()
            cur.execute(self._task.sample_data_sql)
            rows = cur.fetchall()
            cols = [d[0] for d in (cur.description or [])]
            if not rows:
                return None
            header = " | ".join(cols)
            lines  = [header, "-" * len(header)]
            lines += [" | ".join(str(v) for v in r) for r in rows]
            return "\n".join(lines)
        except Exception:
            return None
