"""
Task definitions for the SQL Analyst environment.

Four tasks covering real business SQL use-cases:
  1. simple_select   (easy)      — Filter + sort
  2. aggregate_join  (medium)    — Multi-table JOIN + GROUP BY
  3. window_function (hard)      — CTE + RANK() OVER PARTITION BY
  4. mom_growth      (very_hard) — LAG() + STRFTIME date parsing + CASE WHEN
"""

import sqlite3
from dataclasses import dataclass, field
from typing import List


@dataclass
class Task:
    task_id: str
    difficulty: str
    schema_sql: List[str]
    seed_data_sql: List[str]
    schema_description: str
    question: str
    sample_data_sql: str
    expected_query: str
    max_steps: int
    hints: List[str] = field(default_factory=list)


# ── Task 1: simple_select (easy) ─────────────────────────────────────────────

SIMPLE_SELECT = Task(
    task_id="simple_select",
    difficulty="easy",
    schema_sql=["""CREATE TABLE customers (
        id          INTEGER PRIMARY KEY,
        name        TEXT    NOT NULL,
        city        TEXT    NOT NULL,
        email       TEXT    NOT NULL,
        tier        TEXT    NOT NULL,
        signup_date TEXT    NOT NULL
    )"""],
    seed_data_sql=[
        "INSERT INTO customers VALUES (1,'Alice Johnson','Chicago','alice@acme.com','gold','2022-03-15')",
        "INSERT INTO customers VALUES (2,'Bob Smith','New York','bob@acme.com','silver','2022-07-20')",
        "INSERT INTO customers VALUES (3,'Carol White','Chicago','carol@acme.com','gold','2023-01-10')",
        "INSERT INTO customers VALUES (4,'David Brown','Los Angeles','david@acme.com','bronze','2023-04-05')",
        "INSERT INTO customers VALUES (5,'Eve Davis','Chicago','eve@acme.com','silver','2023-05-22')",
        "INSERT INTO customers VALUES (6,'Frank Wilson','New York','frank@acme.com','gold','2022-11-18')",
        "INSERT INTO customers VALUES (7,'Grace Lee','Chicago','grace@acme.com','bronze','2024-01-03')",
        "INSERT INTO customers VALUES (8,'Henry Clark','Houston','henry@acme.com','gold','2022-06-30')",
        "INSERT INTO customers VALUES (9,'Iris Martinez','Chicago','iris@acme.com','gold','2023-08-14')",
        "INSERT INTO customers VALUES (10,'Jack Anderson','New York','jack@acme.com','silver','2023-09-25')",
    ],
    schema_description="""Table: customers
  id          INTEGER  PRIMARY KEY
  name        TEXT     customer full name
  city        TEXT     customer city
  email       TEXT     contact email
  tier        TEXT     membership tier: 'bronze', 'silver', or 'gold'
  signup_date TEXT     ISO date YYYY-MM-DD""",
    question="List the name and email of all 'gold' tier customers who live in 'Chicago', ordered alphabetically by name.",
    sample_data_sql="SELECT * FROM customers LIMIT 4",
    expected_query="SELECT name, email FROM customers WHERE tier = 'gold' AND city = 'Chicago' ORDER BY name ASC",
    max_steps=5,
    hints=[
        "Use WHERE with two conditions joined by AND",
        "Use ORDER BY name ASC for alphabetical ordering",
        "SELECT only the name and email columns — no extra columns",
    ],
)


# ── Task 2: aggregate_join (medium) ──────────────────────────────────────────

AGGREGATE_JOIN = Task(
    task_id="aggregate_join",
    difficulty="medium",
    schema_sql=[
        """CREATE TABLE products (
            id       INTEGER PRIMARY KEY,
            name     TEXT    NOT NULL,
            category TEXT    NOT NULL,
            price    REAL    NOT NULL
        )""",
        """CREATE TABLE orders (
            id          INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            product_id  INTEGER NOT NULL REFERENCES products(id),
            quantity    INTEGER NOT NULL,
            order_date  TEXT    NOT NULL
        )""",
    ],
    seed_data_sql=[
        "INSERT INTO products VALUES (1,'Laptop Pro','Electronics',1199.99)",
        "INSERT INTO products VALUES (2,'Wireless Mouse','Electronics',29.99)",
        "INSERT INTO products VALUES (3,'Standing Desk','Furniture',499.99)",
        "INSERT INTO products VALUES (4,'Ergonomic Chair','Furniture',349.99)",
        "INSERT INTO products VALUES (5,'Notebook Pack','Stationery',12.99)",
        "INSERT INTO products VALUES (6,'Highlighters','Stationery',8.49)",
        "INSERT INTO products VALUES (7,'Monitor 4K','Electronics',699.99)",
        "INSERT INTO products VALUES (8,'Bookshelf','Furniture',199.99)",
        "INSERT INTO orders VALUES (1,1,1,2,'2024-01-08')",
        "INSERT INTO orders VALUES (2,2,2,5,'2024-01-10')",
        "INSERT INTO orders VALUES (3,3,3,1,'2024-01-15')",
        "INSERT INTO orders VALUES (4,1,4,2,'2024-01-20')",
        "INSERT INTO orders VALUES (5,4,5,20,'2024-02-01')",
        "INSERT INTO orders VALUES (6,2,6,10,'2024-02-05')",
        "INSERT INTO orders VALUES (7,3,1,1,'2024-02-10')",
        "INSERT INTO orders VALUES (8,4,7,3,'2024-02-15')",
        "INSERT INTO orders VALUES (9,5,2,8,'2024-02-20')",
        "INSERT INTO orders VALUES (10,1,8,1,'2024-03-01')",
        "INSERT INTO orders VALUES (11,6,3,2,'2024-03-05')",
        "INSERT INTO orders VALUES (12,5,4,1,'2024-03-10')",
    ],
    schema_description="""Table: products
  id       INTEGER  PRIMARY KEY
  name     TEXT     product name
  category TEXT     'Electronics', 'Furniture', or 'Stationery'
  price    REAL     unit price in USD

Table: orders
  id          INTEGER  PRIMARY KEY
  customer_id INTEGER  customer identifier
  product_id  INTEGER  FK -> products.id
  quantity    INTEGER  units ordered
  order_date  TEXT     ISO date YYYY-MM-DD""",
    question="Find the total revenue (quantity × price) for each product category. Return category and total_revenue, ordered by total_revenue descending.",
    sample_data_sql="SELECT o.id, p.category, p.name, o.quantity, p.price FROM orders o JOIN products p ON o.product_id = p.id LIMIT 4",
    expected_query="SELECT p.category, ROUND(SUM(o.quantity * p.price), 2) AS total_revenue FROM orders o JOIN products p ON o.product_id = p.id GROUP BY p.category ORDER BY total_revenue DESC",
    max_steps=6,
    hints=[
        "JOIN orders with products on o.product_id = p.id",
        "Compute revenue as o.quantity * p.price per row, then SUM per category",
        "Use GROUP BY p.category and ORDER BY total_revenue DESC",
    ],
)


# ── Task 3: window_function (hard) ───────────────────────────────────────────

WINDOW_FUNCTION = Task(
    task_id="window_function",
    difficulty="hard",
    schema_sql=[
        """CREATE TABLE employees (
            id         INTEGER PRIMARY KEY,
            name       TEXT    NOT NULL,
            department TEXT    NOT NULL,
            salary     REAL    NOT NULL
        )""",
        """CREATE TABLE projects (
            id     INTEGER PRIMARY KEY,
            name   TEXT    NOT NULL,
            budget REAL    NOT NULL
        )""",
        """CREATE TABLE employee_projects (
            employee_id  INTEGER NOT NULL REFERENCES employees(id),
            project_id   INTEGER NOT NULL REFERENCES projects(id),
            role         TEXT    NOT NULL,
            hours_worked REAL    NOT NULL,
            PRIMARY KEY (employee_id, project_id)
        )""",
    ],
    seed_data_sql=[
        "INSERT INTO employees VALUES (1,'Alice','Engineering',95000)",
        "INSERT INTO employees VALUES (2,'Bob','Engineering',88000)",
        "INSERT INTO employees VALUES (3,'Carol','Engineering',91000)",
        "INSERT INTO employees VALUES (4,'David','Marketing',75000)",
        "INSERT INTO employees VALUES (5,'Eve','Marketing',72000)",
        "INSERT INTO employees VALUES (6,'Frank','HR',68000)",
        "INSERT INTO employees VALUES (7,'Grace','HR',71000)",
        "INSERT INTO employees VALUES (8,'Hector','Engineering',84000)",
        "INSERT INTO projects VALUES (1,'Alpha',500000)",
        "INSERT INTO projects VALUES (2,'Beta',300000)",
        "INSERT INTO projects VALUES (3,'Gamma',150000)",
        "INSERT INTO employee_projects VALUES (1,1,'Lead',120)",
        "INSERT INTO employee_projects VALUES (1,2,'Contributor',80)",
        "INSERT INTO employee_projects VALUES (2,1,'Lead',160)",
        "INSERT INTO employee_projects VALUES (2,3,'Contributor',40)",
        "INSERT INTO employee_projects VALUES (3,2,'Lead',100)",
        "INSERT INTO employee_projects VALUES (3,3,'Contributor',60)",
        "INSERT INTO employee_projects VALUES (4,2,'Lead',90)",
        "INSERT INTO employee_projects VALUES (4,3,'Contributor',70)",
        "INSERT INTO employee_projects VALUES (5,1,'Contributor',50)",
        "INSERT INTO employee_projects VALUES (5,3,'Lead',110)",
        "INSERT INTO employee_projects VALUES (6,2,'Contributor',45)",
        "INSERT INTO employee_projects VALUES (7,1,'Contributor',30)",
        "INSERT INTO employee_projects VALUES (7,3,'Lead',85)",
        "INSERT INTO employee_projects VALUES (8,1,'Contributor',95)",
        "INSERT INTO employee_projects VALUES (8,2,'Lead',75)",
    ],
    schema_description="""Table: employees
  id         INTEGER  PRIMARY KEY
  name       TEXT     employee name
  department TEXT     'Engineering', 'Marketing', or 'HR'
  salary     REAL     annual salary USD

Table: projects
  id     INTEGER  PRIMARY KEY
  name   TEXT     project name
  budget REAL     project budget USD

Table: employee_projects
  employee_id  INTEGER  FK -> employees.id
  project_id   INTEGER  FK -> projects.id
  role         TEXT     'Lead' or 'Contributor'
  hours_worked REAL     hours spent on this project""",
    question="For each department, find the employee with the highest total hours worked across all projects. Return department, employee_name, and total_hours, ordered by department name.",
    sample_data_sql="SELECT e.name, e.department, ep.project_id, ep.hours_worked FROM employees e JOIN employee_projects ep ON e.id = ep.employee_id LIMIT 5",
    expected_query="""WITH emp_hours AS (
    SELECT e.id, e.name AS employee_name, e.department,
           SUM(ep.hours_worked) AS total_hours
    FROM employees e
    JOIN employee_projects ep ON e.id = ep.employee_id
    GROUP BY e.id, e.name, e.department
),
ranked AS (
    SELECT department, employee_name, total_hours,
           RANK() OVER (PARTITION BY department ORDER BY total_hours DESC) AS rk
    FROM emp_hours
)
SELECT department, employee_name, total_hours
FROM ranked
WHERE rk = 1
ORDER BY department""",
    max_steps=8,
    hints=[
        "First SUM(hours_worked) per employee: GROUP BY employee_id",
        "Use RANK() OVER (PARTITION BY department ORDER BY total_hours DESC) to rank within each department",
        "Wrap in a CTE (WITH clause) and filter WHERE rk = 1",
    ],
)


# ── Task 4: mom_growth (very_hard) ───────────────────────────────────────────

MOM_GROWTH = Task(
    task_id="mom_growth",
    difficulty="very_hard",
    schema_sql=[
        """CREATE TABLE sales (
            id        INTEGER PRIMARY KEY,
            rep_id    INTEGER NOT NULL,
            region    TEXT    NOT NULL,
            product   TEXT    NOT NULL,
            amount    REAL    NOT NULL,
            sale_date TEXT    NOT NULL
        )""",
        """CREATE TABLE reps (
            id         INTEGER PRIMARY KEY,
            name       TEXT    NOT NULL,
            region     TEXT    NOT NULL,
            hire_date  TEXT    NOT NULL,
            manager_id INTEGER
        )""",
    ],
    seed_data_sql=[
        "INSERT INTO reps VALUES (1,'Alice Chen','North','2021-01-15',NULL)",
        "INSERT INTO reps VALUES (2,'Bob Kim','North','2022-03-01',1)",
        "INSERT INTO reps VALUES (3,'Carol Ray','South','2020-06-10',NULL)",
        "INSERT INTO reps VALUES (4,'Dan Park','South','2023-01-20',3)",
        "INSERT INTO reps VALUES (5,'Eve Lin','East','2021-09-01',NULL)",
        "INSERT INTO reps VALUES (6,'Frank Wu','East','2022-11-15',5)",
        "INSERT INTO reps VALUES (7,'Grace Ho','West','2020-04-01',NULL)",
        "INSERT INTO reps VALUES (8,'Henry Ng','West','2023-06-01',7)",
        "INSERT INTO sales VALUES (1,1,'North','Widget',1200.00,'2024-01-05')",
        "INSERT INTO sales VALUES (2,2,'North','Gadget',800.00,'2024-01-12')",
        "INSERT INTO sales VALUES (3,1,'North','Widget',1500.00,'2024-02-03')",
        "INSERT INTO sales VALUES (4,2,'North','Gadget',950.00,'2024-02-18')",
        "INSERT INTO sales VALUES (5,1,'North','Widget',1800.00,'2024-03-07')",
        "INSERT INTO sales VALUES (6,2,'North','Gadget',1100.00,'2024-03-22')",
        "INSERT INTO sales VALUES (7,3,'South','Widget',2000.00,'2024-01-08')",
        "INSERT INTO sales VALUES (8,4,'South','Gadget',600.00,'2024-01-19')",
        "INSERT INTO sales VALUES (9,3,'South','Widget',1800.00,'2024-02-11')",
        "INSERT INTO sales VALUES (10,4,'South','Gadget',750.00,'2024-02-25')",
        "INSERT INTO sales VALUES (11,3,'South','Widget',2200.00,'2024-03-14')",
        "INSERT INTO sales VALUES (12,4,'South','Gadget',900.00,'2024-03-29')",
        "INSERT INTO sales VALUES (13,5,'East','Widget',900.00,'2024-01-03')",
        "INSERT INTO sales VALUES (14,6,'East','Gadget',400.00,'2024-01-22')",
        "INSERT INTO sales VALUES (15,5,'East','Widget',1100.00,'2024-02-07')",
        "INSERT INTO sales VALUES (16,6,'East','Gadget',500.00,'2024-02-28')",
        "INSERT INTO sales VALUES (17,5,'East','Widget',1300.00,'2024-03-10')",
        "INSERT INTO sales VALUES (18,6,'East','Gadget',600.00,'2024-03-25')",
        "INSERT INTO sales VALUES (19,7,'West','Widget',3000.00,'2024-01-06')",
        "INSERT INTO sales VALUES (20,8,'West','Gadget',500.00,'2024-01-15')",
        "INSERT INTO sales VALUES (21,7,'West','Widget',2800.00,'2024-02-09')",
        "INSERT INTO sales VALUES (22,8,'West','Gadget',700.00,'2024-02-20')",
        "INSERT INTO sales VALUES (23,7,'West','Widget',3500.00,'2024-03-12')",
        "INSERT INTO sales VALUES (24,8,'West','Gadget',800.00,'2024-03-28')",
    ],
    schema_description="""Table: sales
  id        INTEGER  PRIMARY KEY
  rep_id    INTEGER  FK -> reps.id
  region    TEXT     'North', 'South', 'East', 'West'
  product   TEXT     product name
  amount    REAL     sale amount USD
  sale_date TEXT     ISO date YYYY-MM-DD

Table: reps
  id         INTEGER  PRIMARY KEY
  name       TEXT     rep full name
  region     TEXT     assigned region
  hire_date  TEXT     ISO date YYYY-MM-DD
  manager_id INTEGER  FK -> reps.id (NULL for top-level managers)""",
    question=(
        "For each region, calculate the month-over-month revenue growth rate for Q1 2024 "
        "(January, February, March). Return region, month (as '01','02','03'), revenue "
        "(total sales amount for that month), and growth_pct (percentage change vs prior month, "
        "rounded to 2 decimal places — NULL for January since there is no prior month). "
        "Order by region then month."
    ),
    sample_data_sql="SELECT region, sale_date, amount FROM sales ORDER BY region, sale_date LIMIT 6",
    expected_query="""WITH monthly AS (
    SELECT region,
           STRFTIME('%m', sale_date) AS month,
           SUM(amount) AS revenue
    FROM sales
    WHERE sale_date BETWEEN '2024-01-01' AND '2024-03-31'
    GROUP BY region, STRFTIME('%m', sale_date)
),
with_lag AS (
    SELECT region, month, revenue,
           LAG(revenue) OVER (PARTITION BY region ORDER BY month) AS prev_revenue
    FROM monthly
)
SELECT region,
       month,
       revenue,
       CASE WHEN prev_revenue IS NULL THEN NULL
            ELSE ROUND((revenue - prev_revenue) * 100.0 / prev_revenue, 2)
       END AS growth_pct
FROM with_lag
ORDER BY region, month""",
    max_steps=10,
    hints=[
        "Use STRFTIME('%m', sale_date) to extract month; filter WHERE sale_date BETWEEN '2024-01-01' AND '2024-03-31'",
        "GROUP BY region and month, compute SUM(amount) AS revenue",
        "Use LAG(revenue) OVER (PARTITION BY region ORDER BY month) to get the previous month's revenue",
        "Wrap in CTEs: first aggregate monthly revenue, then apply LAG, then compute CASE WHEN growth_pct",
    ],
)


TASKS: dict = {
    "simple_select":   SIMPLE_SELECT,
    "aggregate_join":  AGGREGATE_JOIN,
    "window_function": WINDOW_FUNCTION,
    "mom_growth":      MOM_GROWTH,
}


def build_db(task: Task) -> sqlite3.Connection:
    """Create an in-memory SQLite database seeded with task data."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    for stmt in task.schema_sql:
        conn.execute(stmt)
    for stmt in task.seed_data_sql:
        conn.execute(stmt)
    conn.commit()
    return conn
