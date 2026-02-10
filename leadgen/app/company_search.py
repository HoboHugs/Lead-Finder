from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Literal

import duckdb


@dataclass(frozen=True)
class DatasetInfo:
    base_dir: str
    glob_path: str


def _to_duckdb_path(p: str) -> str:
    # DuckDB prefers forward slashes; also avoids backslash escape issues on Windows.
    return p.replace("\\", "/")


@lru_cache(maxsize=1)
def get_dataset_info() -> DatasetInfo:
    """Resolve dataset location.

    Priority:
      1) FLORIDA_BIZ_DIR env var
      2) default: ../../Florida Biz (relative to this file via cwd at runtime)

    Note: In many deployments (Render), these large CSVs won't be present unless you mount them.
    """

    env_dir = (os.getenv("FLORIDA_BIZ_DIR") or "").strip()
    if env_dir:
        base = env_dir
    else:
        # project layout: leadgen/app/company_search.py; dataset is at Lead-Finder/Florida Biz
        # runtime CWD is leadgen/ (per main.py reading static via relative path)
        base = os.path.abspath(os.path.join(os.getcwd(), "..", "Florida Biz"))

    glob_path = os.path.join(base, "companies_categorized_*.csv")
    return DatasetInfo(base_dir=base, glob_path=glob_path)


@lru_cache(maxsize=1)
def get_conn() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    # Reasonable defaults for interactive querying.
    con.execute("PRAGMA threads=4")
    return con


def dataset_exists() -> bool:
    info = get_dataset_info()
    return os.path.isdir(info.base_dir) and any(
        name.startswith("companies_categorized_") and name.endswith(".csv")
        for name in os.listdir(info.base_dir)
    )


def ensure_view() -> None:
    """Create/replace a view that reads all years' CSVs via glob."""
    info = get_dataset_info()
    con = get_conn()

    # Use filename=true so we can derive year.
    glob_path = _to_duckdb_path(info.glob_path)

    # DuckDB does not allow parameterized paths in CREATE VIEW statements.
    safe_path = glob_path.replace("'", "''")

    con.execute(
        f"""
        CREATE OR REPLACE VIEW florida_companies AS
        SELECT
          CASE
            WHEN doc_number IS NOT NULL AND doc_number LIKE 'L%' AND length(doc_number) >= 3
              THEN '20' || substr(doc_number, 2, 2)
            ELSE NULL
          END AS year,
          doc_number,
          company_name,
          entity_code,
          address,
          city,
          zipcode,
          registered_agent_name,
          people_roles,
          source_file,
          category,
          category_reason
        FROM read_csv_auto('{safe_path}', filename=true, header=true, ignore_errors=true)
        """
    )


def list_years() -> list[str]:
    ensure_view()
    con = get_conn()
    rows = con.execute(
        "SELECT DISTINCT year FROM florida_companies WHERE year IS NOT NULL AND year != '' ORDER BY year"
    ).fetchall()
    return [r[0] for r in rows]


def list_categories(limit: int = 5000) -> list[str]:
    ensure_view()
    con = get_conn()
    rows = con.execute(
        """
        SELECT DISTINCT category
        FROM florida_companies
        WHERE category IS NOT NULL AND category != ''
        ORDER BY category
        LIMIT ?
        """,
        [limit],
    ).fetchall()
    return [r[0] for r in rows]


def search_companies(
    *,
    year: str | None = None,
    category: str | None = None,
    city: str | None = None,
    zipcode: str | None = None,
    entity_code: str | None = None,
    q: str | None = None,
    limit: int = 50,
    offset: int = 0,
    sort: Literal["company_name", "city", "year"] = "company_name",
) -> dict[str, Any]:
    """Search with basic filters.

    Returns: {results: [...], has_more: bool}
    """

    ensure_view()
    con = get_conn()

    where = []
    params: list[Any] = []

    def add_eq(col: str, val: str | None):
        if val is None:
            return
        v = val.strip()
        if not v:
            return
        where.append(f"lower({col}) = lower(?)")
        params.append(v)

    add_eq("year", year)
    add_eq("category", category)
    add_eq("city", city)
    add_eq("zipcode", zipcode)
    add_eq("entity_code", entity_code)

    if q and q.strip():
        # match company_name/address/agent/roles
        where.append(
            "(" +
            " OR ".join(
                [
                    "lower(company_name) LIKE lower(?)",
                    "lower(address) LIKE lower(?)",
                    "lower(registered_agent_name) LIKE lower(?)",
                    "lower(people_roles) LIKE lower(?)",
                ]
            )
            + ")"
        )
        like = f"%{q.strip()}%"
        params.extend([like, like, like, like])

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    # Fetch one extra row to determine has_more.
    lim = max(1, min(int(limit), 200))
    off = max(0, int(offset))

    sort_col = {
        "company_name": "company_name",
        "city": "city",
        "year": "year",
    }[sort]

    sql = f"""
    SELECT year, doc_number, company_name, entity_code, address, city, zipcode,
           registered_agent_name, people_roles, source_file, category
    FROM florida_companies
    {where_sql}
    ORDER BY {sort_col} NULLS LAST
    LIMIT ? OFFSET ?
    """

    rows = con.execute(sql, params + [lim + 1, off]).fetchall()

    cols = [
        "year",
        "doc_number",
        "company_name",
        "entity_code",
        "address",
        "city",
        "zipcode",
        "registered_agent_name",
        "people_roles",
        "source_file",
        "category",
    ]

    results = [dict(zip(cols, r)) for r in rows[:lim]]
    return {"results": results, "has_more": len(rows) > lim}


def export_csv_text(**kwargs: Any) -> str:
    """Export the filtered results as CSV (bounded to a sane max)."""
    ensure_view()
    con = get_conn()

    # Reuse search to build WHERE + params by calling search with high limit,
    # but do the export in SQL for speed.
    year = kwargs.get("year")
    category = kwargs.get("category")
    city = kwargs.get("city")
    zipcode = kwargs.get("zipcode")
    entity_code = kwargs.get("entity_code")
    q = kwargs.get("q")
    max_rows = max(1, min(int(kwargs.get("max_rows") or 5000), 50000))

    where = []
    params: list[Any] = []

    def add_eq(col: str, val: str | None):
        if val is None:
            return
        v = str(val).strip()
        if not v:
            return
        where.append(f"lower({col}) = lower(?)")
        params.append(v)

    add_eq("year", year)
    add_eq("category", category)
    add_eq("city", city)
    add_eq("zipcode", zipcode)
    add_eq("entity_code", entity_code)

    if q and str(q).strip():
        where.append(
            "(" +
            " OR ".join(
                [
                    "lower(company_name) LIKE lower(?)",
                    "lower(address) LIKE lower(?)",
                    "lower(registered_agent_name) LIKE lower(?)",
                    "lower(people_roles) LIKE lower(?)",
                ]
            )
            + ")"
        )
        like = f"%{str(q).strip()}%"
        params.extend([like, like, like, like])

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
    SELECT year, doc_number, company_name, entity_code, address, city, zipcode,
           registered_agent_name, people_roles, source_file, category
    FROM florida_companies
    {where_sql}
    ORDER BY company_name NULLS LAST
    LIMIT ?
    """

    rel = con.execute(sql, params + [max_rows])
    return rel.df().to_csv(index=False)
