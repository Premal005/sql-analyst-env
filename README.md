---
title: SQL Analyst OpenEnv
emoji: 🗄️
colorFrom: blue
colorTo: indigo
sdk: docker
pinned: false
tags:
  - openenv
  - sql
  - text-to-sql
  - business-intelligence
  - agent-evaluation
license: mit
app_port: 7860
---

# SQL Analyst OpenEnv 🗄️

An [OpenEnv](https://openenv.dev) environment where an AI agent acts as a **SQL data analyst**,
writing queries against an in-memory SQLite database to answer natural-language business questions.

**Why this matters:** Text-to-SQL is one of the most commercially deployed LLM capabilities —
used in BI tools, data chatbots, and enterprise analytics. This environment lets you train and
evaluate agents on a real, measurable, high-value skill with deterministic correctness criteria.

---

## Quick API Reference

| Method | Endpoint | Body | Description |
|--------|----------|------|-------------|
| `POST` | `/reset` | `{"task_id": "simple_select"}` | Start new episode |
| `POST` | `/step`  | `{"query": "SELECT ..."}` | Submit SQL query |
| `GET`  | `/state` | — | Current episode state |
| `GET`  | `/health`| — | Liveness probe |

---

## Action Space

```json
{ "query": "<SQL SELECT string>" }
```

Any valid SQLite `SELECT` statement. The agent may run exploratory queries
(e.g. `SELECT * FROM customers LIMIT 3`) to understand the data before writing the final answer.

---

## Observation Space

| Field | Type | Description |
|-------|------|-------------|
| `task_id` | string | Active task identifier |
| `difficulty` | string | `easy` / `medium` / `hard` / `very_hard` |
| `schema_description` | string | Human-readable table definitions |
| `question` | string | Natural-language business question |
| `sample_data` | string? | First 4–6 rows of key tables |
| `last_query` | string? | Agent's most recent SQL submission |
| `last_result` | string? | Preview of query output (≤10 rows) |
| `last_error` | string? | SQL error message if any |
| `last_score` | float? | Quality score for last attempt [0.0, 1.0] |
| `step_number` | int | Current step index |
| `max_steps` | int | Maximum steps per episode |
| `hint` | string? | Progressive hint, unlocked after step 3 if score < 0.5 |

---

## Reward Function

```
score  = col_match × 0.25 + row_F1 × 0.75 + order_bonus × 0.05
reward = score × max(0.5, 1.0 − 0.05 × (step − 1))  −  0.10 × [syntax_error]
```

- **col_match** — fraction of expected output columns present in agent result
- **row_F1** — F1 between agent rows and expected rows (multiset-based, order-insensitive)
- **order_bonus** — +0.05 bonus if row ordering also matches
- **decay** — rewards decay slightly per step to incentivise correct early solutions
- **error_penalty** — −0.10 for SQL syntax errors (floors at 0.0)
- `done = True` when score ≥ 0.95 **or** max_steps reached

---

## Tasks

### 1. `simple_select` — *Easy* (max 5 steps)
**Schema:** `customers(id, name, city, email, tier, signup_date)`
**Question:** List the name and email of all 'gold' tier customers in 'Chicago', alphabetically.
**Skills tested:** `WHERE` (multi-condition AND), `ORDER BY`
**Baseline (Qwen2.5-72B):** 1.000 in 1 step

### 2. `aggregate_join` — *Medium* (max 6 steps)
**Schema:** `products(id, name, category, price)` · `orders(id, customer_id, product_id, quantity, order_date)`
**Question:** Find total revenue (quantity × price) per product category, ordered by revenue desc.
**Skills tested:** `JOIN`, `GROUP BY`, `SUM`, `ROUND`, `ORDER BY`
**Baseline (Qwen2.5-72B):** 1.000 in 1 step

### 3. `window_function` — *Hard* (max 8 steps)
**Schema:** `employees` · `projects` · `employee_projects(employee_id, project_id, role, hours_worked)`
**Question:** For each department, find the employee with the highest total hours worked.
**Skills tested:** CTE (`WITH`), `RANK() OVER (PARTITION BY … ORDER BY …)`, multi-table JOIN
**Baseline (Qwen2.5-72B):** 1.000 in 1 step

### 4. `mom_growth` — *Very Hard* (max 10 steps)
**Schema:** `sales(id, rep_id, region, product, amount, sale_date)` · `reps(id, name, region, hire_date, manager_id)`
**Question:** For each region, compute month-over-month revenue growth rate for Q1 2024. Return region, month, revenue, and growth_pct (NULL for January).
**Skills tested:** `STRFTIME` date parsing, `LAG()` window function, `CASE WHEN NULL`, multi-CTE
**Baseline (Qwen2.5-72B):** ~0.50 — this task genuinely challenges frontier models

---

## Baseline Scores

| Task | Model | Score | Steps |
|------|-------|-------|-------|
| `simple_select` | Qwen/Qwen2.5-72B-Instruct | **1.000** | 1 |
| `aggregate_join` | Qwen/Qwen2.5-72B-Instruct | **1.000** | 1 |
| `window_function` | Qwen/Qwen2.5-72B-Instruct | **1.000** | 1 |
| `mom_growth` | Qwen/Qwen2.5-72B-Instruct | **~0.500** | 3–5 |
| **Overall** | | **~0.875** | |

---

## Setup & Running

### Local development
```bash
pip install openenv-core[core]
cd sql_analyst_env
uv run server
```

### Docker
```bash
docker build -t sql-analyst-env .
docker run -p 7860:7860 -e SQL_ENV_TASK=simple_select sql-analyst-env
```

### Run the inference baseline
```bash
export HF_TOKEN="hf_..."
export MODEL_NAME="Qwen/Qwen2.5-72B-Instruct"
export API_BASE_URL="https://router.huggingface.co/v1"
export SQL_ENV_URL="https://PremalGoyal-sql-analyst-env.hf.space"
python inference.py
```

---

## Task Selection

Pass `SQL_ENV_TASK` env var to run a specific task:
```bash
SQL_ENV_TASK=mom_growth python inference.py
```

Or pass `task_id` in the `/reset` body:
```bash
curl -X POST http://localhost:7860/reset \
  -H "Content-Type: application/json" \
  -d '{"task_id": "mom_growth"}'
```
