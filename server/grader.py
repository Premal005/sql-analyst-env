
"""
Grading logic for the SQL Analyst environment.

Scoring:
  score = col_match * 0.25 + row_f1 * 0.75 + order_bonus * 0.05
  
  col_match  = fraction of expected columns present in agent result
  row_f1     = F1 between agent rows and expected rows (multiset-based)
  order_bonus = +0.05 if row ordering also matches exactly

Reward per step:
  reward = score * max(0.5, 1.0 - 0.05 * (step - 1))
         - 0.10 if SQL syntax error (floor at 0.0)
"""

import sqlite3
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple


def _normalize_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float):
        return round(v, 2)
    if isinstance(v, int):
        return float(v)
    return str(v).strip().lower()


def _normalize_row(row) -> tuple:
    return tuple(_normalize_value(v) for v in row)


def _run_query(conn: sqlite3.Connection, sql: str) -> Tuple[List, List[str], Optional[str]]:
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0].lower().strip() for d in (cur.description or [])]
        return rows, cols, None
    except Exception as exc:
        return [], [], str(exc)


def grade(conn: sqlite3.Connection, agent_query: str, expected_query: str) -> Dict[str, Any]:
    """
    Grade the agent's SQL query vs the reference solution.
    Returns dict with: score, error, result_preview, breakdown.
    """
    exp_rows, exp_cols, exp_err = _run_query(conn, expected_query)
    if exp_err:
        return {"score": 0.5, "error": None, "result_preview": None, "breakdown": {}}

    agent_rows, agent_cols, agent_err = _run_query(conn, agent_query)

    if agent_err:
        return {
            "score": 0.01,
            "error": agent_err,
            "result_preview": None,
            "breakdown": {"syntax_error": True},
        }

    # Column score
    exp_col_set   = set(exp_cols)
    agent_col_set = set(agent_cols)
    col_score = len(exp_col_set & agent_col_set) / len(exp_col_set) if exp_col_set else 0.0

    # Row F1 (multiset-based)
    norm_agent = [_normalize_row(r) for r in agent_rows]
    norm_exp   = [_normalize_row(r) for r in exp_rows]

    if not norm_exp:
        row_score = 0.0
    else:
        agent_ctr = Counter(norm_agent)
        exp_ctr   = Counter(norm_exp)
        inter     = sum(min(agent_ctr[k], exp_ctr[k]) for k in exp_ctr)
        precision = inter / len(norm_agent) if norm_agent else 0.0
        recall    = inter / len(norm_exp)
        row_score = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0

    # Order bonus
    order_bonus = 0.05 if (norm_agent and norm_exp and norm_agent == norm_exp) else 0.0

    raw_score = col_score * 0.25 + row_score * 0.75 + order_bonus
    # Clamp strictly between 0 and 1 (exclusive) — validator rejects 0.0 and 1.0
    score = round(max(0.01, min(0.99, raw_score)), 4)

    # Result preview
    preview_rows = agent_rows[:10]
    if preview_rows:
        header = " | ".join(agent_cols)
        lines  = [header, "-" * max(len(header), 4)]
        lines += [" | ".join(str(v) for v in r) for r in preview_rows]
        if len(agent_rows) > 10:
            lines.append(f"... ({len(agent_rows)} rows total)")
        result_preview = "\n".join(lines)
    else:
        result_preview = "(query returned no rows)"

    return {
        "score": score,
        "error": None,
        "result_preview": result_preview,
        "breakdown": {
            "col_score":    round(col_score, 4),
            "row_score":    round(row_score, 4),
            "order_bonus":  order_bonus,
            "agent_rows":   len(agent_rows),
            "expected_rows": len(exp_rows),
        },
    }
