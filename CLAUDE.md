# CLAUDE.md

## Project Overview

`rdbms-metadata-manager` is a **read-only** database schema analysis tool. It connects to a MySQL or PostgreSQL database, reflects its schema using SQLAlchemy, and runs a set of detection rules to identify quality issues (normalization problems, missing indexes, wrong data types, nullable violations). Results are printed to the console and optionally exported to CSV.

No data is written to or modified in the target database.

## Running the Application

**With Docker (recommended):**
```bash
docker build -t rdbms-metadata-manager .
docker run --env-file .env -v $(pwd)/exports:/app/exports rdbms-metadata-manager
```

**With Docker (same network as the database container):**
```bash
docker run --env-file .env --network <docker_network> -v $(pwd)/exports:/app/exports rdbms-metadata-manager
```

**Directly with Python:**
```bash
pip install -r requirements.txt
python app.py
```

## Architecture

- **`app.py`** — entire application logic (no other source files)
- **`detect_schema_flaws(engine)`** — reflects schema via SQLAlchemy `MetaData` + `Inspector`, iterates all tables and columns, evaluates each against the 5 rules, returns a list of issue dicts
- **`export_to_csv(issues, filename)`** — writes the issue list to a CSV file under `exports/`
- **`get_database_name(db_url)`** — parses the DB name from the connection URL

## Environment Variables

Loaded from a `.env` file via `python-dotenv`. See `.env.example` for a template.

| Variable | Required | Description |
|----------|----------|-------------|
| `DB_URL` | Yes | SQLAlchemy connection string for MySQL or PostgreSQL |
| `EXPORT_TO_CSV` | No | Set to `YES` to export results to `exports/<db_name>_schema_issues.csv` |

## Detection Rules

All rules live inside `detect_schema_flaws()` in `app.py`. Each rule appends a dict to the `issues` list.

| # | Name | Condition |
|---|------|-----------|
| 1 | Query performance – missing index | VARCHAR/TEXT column with length ≥ 255, not indexed, not unique |
| 2 | Normalization – data integrity | Column name starts or ends with "id", no FK constraint, not a PK |
| 3 | Data type – precision error | Column name contains price/amount/total/cost/value/balance/rate, type is not DECIMAL/NUMERIC |
| 4 | Data type mismatch | `rating` → FLOAT, `created_at` → DATETIME, `order_date` → DATETIME |
| 5 | Data integrity – NULL values | `email`, `price`, `total_amount`, `order_date`, `rating` allow NULL |

## Adding a New Detection Rule

1. Open `app.py` and locate the `for column in table.columns:` loop inside `detect_schema_flaws()`
2. Add a new `if` block that evaluates `column`, `column_type`, `indexed_columns`, `foreign_key_columns`, etc.
3. Append to `issues` using this exact structure:

```python
issues.append({
    "table": table_name,
    "column": column.name,
    "issue type": "<Category> - <Sub-category>",
    "issue": "<Human-readable description of the problem>",
    "recommendation": "<Actionable fix recommendation>"
})
```

## Issue Dict Structure

Every issue must have exactly these five keys (lowercase, with spaces):

```python
{
    "table": str,         # table name as reflected by SQLAlchemy
    "column": str,        # column name
    "issue type": str,    # e.g. "Data type mismatch"
    "issue": str,         # description of the problem
    "recommendation": str # actionable fix
}
```

## Constraints

- **Read-only**: never write to or alter the target database
- **No ORM models**: schema is discovered dynamically via reflection, not pre-defined models
- **No test suite yet**: manual verification is done by connecting to a real database
