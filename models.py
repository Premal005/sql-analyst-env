"""Data models for the SQL Analyst environment."""

from typing import Optional
from openenv.core.env_server.types import Action, Observation
from pydantic import Field


class SqlAnalystAction(Action):
    """Action: a SQL query string submitted by the agent."""
    query: str = Field(..., description="SQL SELECT query to execute against the task database")


class SqlAnalystObservation(Observation):
    """Observation returned after reset() and each step()."""
    task_id: str = Field(default="", description="Active task identifier")
    difficulty: str = Field(default="easy", description="easy | medium | hard | very_hard")
    schema_description: str = Field(default="", description="Human-readable table schema")
    question: str = Field(default="", description="Natural-language business question to answer")
    sample_data: Optional[str] = Field(default=None, description="Sample rows from key tables")
    last_query: Optional[str] = Field(default=None, description="Agent's most recent SQL query")
    last_result: Optional[str] = Field(default=None, description="Preview of query result (<=10 rows)")
    last_error: Optional[str] = Field(default=None, description="SQL error message if any")
    last_score: Optional[float] = Field(default=None, description="Score for last attempt [0.0, 1.0]")
    step_number: int = Field(default=0, description="Current step index")
    max_steps: int = Field(default=5, description="Maximum steps per episode")
    hint: Optional[str] = Field(default=None, description="Progressive hint unlocked after step 3")
