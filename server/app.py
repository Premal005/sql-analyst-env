"""FastAPI application for the SQL Analyst environment."""

try:
    from openenv.core.env_server.http_server import create_app
except Exception as e:
    raise ImportError("openenv-core required — run: pip install openenv-core[core]") from e

try:
    from ..models import SqlAnalystAction, SqlAnalystObservation
    from .sql_analyst_env_environment import SqlAnalystEnvironment
except ImportError:
    from models import SqlAnalystAction, SqlAnalystObservation
    from server.sql_analyst_env_environment import SqlAnalystEnvironment

app = create_app(
    SqlAnalystEnvironment,
    SqlAnalystAction,
    SqlAnalystObservation,
    env_name="sql_analyst_env",
    max_concurrent_envs=4,
)


def main(host: str = "0.0.0.0", port: int = 7860):
    """Entry point: uv run server  or  python -m sql_analyst_env.server.app"""
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
