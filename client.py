"""SQL Analyst Environment Client."""

from typing import Dict

from openenv.core import EnvClient
from openenv.core.client_types import StepResult
from openenv.core.env_server.types import State

from .models import SqlAnalystAction, SqlAnalystObservation


class SqlAnalystEnv(EnvClient[SqlAnalystAction, SqlAnalystObservation, State]):
    """
    Client for the SQL Analyst OpenEnv environment.

    Connects via WebSocket for low-latency multi-step interaction.

    Example:
        >>> with SqlAnalystEnv(base_url="http://localhost:7860") as env:
        ...     result = env.reset()
        ...     obs = result.observation
        ...     result = env.step(SqlAnalystAction(query="SELECT name FROM customers"))
    """

    def _step_payload(self, action: SqlAnalystAction) -> Dict:
        return {"query": action.query}

    def _parse_result(self, payload: Dict) -> StepResult[SqlAnalystObservation]:
        obs_data = payload.get("observation", {})
        observation = SqlAnalystObservation(
            task_id=obs_data.get("task_id", ""),
            difficulty=obs_data.get("difficulty", "easy"),
            schema_description=obs_data.get("schema_description", ""),
            question=obs_data.get("question", ""),
            sample_data=obs_data.get("sample_data"),
            last_query=obs_data.get("last_query"),
            last_result=obs_data.get("last_result"),
            last_error=obs_data.get("last_error"),
            last_score=obs_data.get("last_score"),
            step_number=obs_data.get("step_number", 0),
            max_steps=obs_data.get("max_steps", 5),
            hint=obs_data.get("hint"),
            done=payload.get("done", False),
            reward=payload.get("reward", 0.0),
        )
        return StepResult(
            observation=observation,
            reward=payload.get("reward", 0.0),
            done=payload.get("done", False),
        )

    def _parse_state(self, payload: Dict) -> State:
        return State(
            episode_id=payload.get("episode_id"),
            step_count=payload.get("step_count", 0),
        )
