# """
# SQL Analyst Environment — Core Implementation
# """

# import os
# import sqlite3
# from typing import Optional
# from uuid import uuid4

# from openenv.core.env_server.interfaces import Environment
# from openenv.core.env_server.types import State

# try:
#     from ..models import SqlAnalystAction, SqlAnalystObservation
# except ImportError:
#     from models import SqlAnalystAction, SqlAnalystObservation

# try:
#     from .tasks import TASKS, Task, build_db
#     from .grader import grade
# except ImportError:
#     from tasks import TASKS, Task, build_db
#     from grader import grade

# DONE_THRESHOLD = 0.95
# STEP_DECAY     = 0.05


# class SqlAnalystEnvironment(Environment):
#     """SQL Analyst environment for evaluating Text-to-SQL agents."""

#     SUPPORTS_CONCURRENT_SESSIONS: bool = True

#     def __init__(self):
#         self._conn: Optional[sqlite3.Connection] = None
#         self._task: Optional[Task] = None
#         self._step_number: int = 0
#         self._done: bool = False
#         self._best_score: float = 0.0
#         self._last_obs: Optional[SqlAnalystObservation] = None
#         self._state = State(episode_id=str(uuid4()), step_count=0)

#     def _get_task(self) -> Task:
#         task_id = os.getenv("SQL_ENV_TASK", "simple_select")
#         return TASKS.get(task_id, TASKS["simple_select"])

#     def reset(self) -> SqlAnalystObservation:
#         self._task = self._get_task()
#         if self._conn:
#             try:
#                 self._conn.close()
#             except Exception:
#                 pass
#         self._conn        = build_db(self._task)
#         self._step_number = 0
#         self._done        = False
#         self._best_score  = 0.0
#         self._state       = State(episode_id=str(uuid4()), step_count=0)
#         obs = SqlAnalystObservation(
#             task_id=self._task.task_id,
#             difficulty=self._task.difficulty,
#             schema_description=self._task.schema_description,
#             question=self._task.question,
#             sample_data=self._get_sample(),
#             step_number=0,
#             max_steps=self._task.max_steps,
#             done=False,
#             reward=0.0,
#         )
#         self._last_obs = obs
#         return obs

#     def step(self, action: SqlAnalystAction) -> SqlAnalystObservation:
#         """Called by openenv framework (WebSocket path)."""
#         return self.step_with_query(action.query)

#     def step_with_query(self, query: str) -> SqlAnalystObservation:
#         """Called directly by our HTTP /step endpoint."""
#         if self._task is None:
#             self.reset()
#         if self._done:
#             return self._last_obs

#         self._step_number      += 1
#         self._state.step_count += 1

#         result  = grade(self._conn, query.strip(), self._task.expected_query)
#         score   = result["score"]
#         error   = result["error"]
#         preview = result["result_preview"]

#         decay  = max(0.5, 1.0 - STEP_DECAY * (self._step_number - 1))
#         reward = max(round(score * decay - (0.10 if error else 0.0), 4), 0.0)

#         self._best_score = max(self._best_score, score)
#         self._done = (score >= DONE_THRESHOLD) or (self._step_number >= self._task.max_steps)

#         hint: Optional[str] = None
#         if self._step_number >= 3 and score < 0.5 and self._task.hints:
#             idx  = min(self._step_number - 3, len(self._task.hints) - 1)
#             hint = f"Hint {idx + 1}: {self._task.hints[idx]}"

#         obs = SqlAnalystObservation(
#             task_id=self._task.task_id,
#             difficulty=self._task.difficulty,
#             schema_description=self._task.schema_description,
#             question=self._task.question,
#             sample_data=self._last_obs.sample_data if self._last_obs else None,
#             last_query=query.strip(),
#             last_result=preview,
#             last_error=error,
#             last_score=score,
#             step_number=self._step_number,
#             max_steps=self._task.max_steps,
#             hint=hint,
#             done=self._done,
#             reward=reward,
#         )
#         self._last_obs = obs
#         return obs

#     @property
#     def state(self) -> State:
#         return self._state

#     def _get_sample(self) -> Optional[str]:
#         try:
#             cur  = self._conn.cursor()
#             cur.execute(self._task.sample_data_sql)
#             rows = cur.fetchall()
#             cols = [d[0] for d in (cur.description or [])]
#             if not rows:
#                 return None
#             header = " | ".join(cols)
#             lines  = [header, "-" * len(header)]
#             lines += [" | ".join(str(v) for v in r) for r in rows]
#             return "\n".join(lines)
#         except Exception:
#             return None


"""
SQL Analyst Environment — Core Implementation
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

DONE_THRESHOLD = 0.95
STEP_DECAY     = 0.05


class SqlAnalystEnvironment(Environment):
    """SQL Analyst environment for evaluating Text-to-SQL agents."""

    SUPPORTS_CONCURRENT_SESSIONS: bool = True

    def __init__(self):
        self._conn: Optional[sqlite3.Connection] = None
        self._task: Optional[Task] = None
        self._step_number: int = 0
        self._done: bool = False
        self._best_score: float = 0.0
        self._last_obs: Optional[SqlAnalystObservation] = None
        self._state = State(episode_id=str(uuid4()), step_count=0)

    def _get_task(self) -> Task:
        task_id = os.getenv("SQL_ENV_TASK", "simple_select")
        return TASKS.get(task_id, TASKS["simple_select"])

    def reset(self) -> SqlAnalystObservation:
        self._task = self._get_task()
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
        self._conn        = build_db(self._task)
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
        """Called by openenv framework (WebSocket path)."""
        return self.step_with_query(action.query)

    def step_with_query(self, query: str) -> SqlAnalystObservation:
        """Called directly by our HTTP /step endpoint."""
        if self._task is None:
            self.reset()
        if self._done:
            return self._last_obs

        self._step_number      += 1
        self._state.step_count += 1

        result  = grade(self._conn, query.strip(), self._task.expected_query)
        score   = result["score"]
        error   = result["error"]
        preview = result["result_preview"]

        decay  = max(0.5, 1.0 - STEP_DECAY * (self._step_number - 1))
        reward = max(round(score * decay - (0.10 if error else 0.0), 4), 0.0)
        # Validator requires reward strictly between 0 and 1 (exclusive)
        reward = max(0.001, min(0.999, reward))

        self._best_score = max(self._best_score, score)
        self._done = (score >= DONE_THRESHOLD) or (self._step_number >= self._task.max_steps)

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
            last_query=query.strip(),
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

    def _get_sample(self) -> Optional[str]:
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
