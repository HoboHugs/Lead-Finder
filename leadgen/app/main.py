from __future__ import annotations

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import HTMLResponse, PlainTextResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .scraper import run_contacts, results_to_csv_text
from .company_search import (
    dataset_exists,
    export_csv_text,
    list_categories,
    list_years,
    search_companies,
)

import os

app = FastAPI(title="Lead Gen Scraper", version="0.1.0")

app.mount("/static", StaticFiles(directory="static"), name="static")


def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    """Shared-secret auth for the public API.

    Supports either:
      - API_KEY: a single key
      - API_KEYS: comma-separated list of keys (use this for per-person keys)

    Clients must send header: X-API-Key: <value>

    If neither API_KEY nor API_KEYS is set, auth is disabled.
    """

    single = (os.getenv("API_KEY") or "").strip()
    multi_raw = (os.getenv("API_KEYS") or "").strip()

    allowed: set[str] = set()
    if single:
        allowed.add(single)
    if multi_raw:
        allowed.update({k.strip() for k in multi_raw.split(",") if k.strip()})

    if not allowed:
        return

    if not x_api_key or x_api_key.strip() not in allowed:
        raise HTTPException(status_code=401, detail="Unauthorized")


class LeadRequest(BaseModel):
    companies_text: str = Field(..., description="Company names, one per line")
    sleep_between: float = Field(0.5, ge=0.0, le=5.0)
    per_page_delay_s: float = Field(0.2, ge=0.0, le=2.0)
    max_companies: int = Field(50, ge=1, le=200)


@app.get("/", response_class=HTMLResponse)
def index():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()


@app.get("/companies", response_class=HTMLResponse)
def companies_page():
    with open("static/companies.html", "r", encoding="utf-8") as f:
        return f.read()


@app.get("/api/companies/filters")
def companies_filters(_: None = Depends(require_api_key)):
    if not dataset_exists():
        return {
            "dataset_ok": False,
            "message": "Florida Biz dataset not found on server. Set FLORIDA_BIZ_DIR or mount the CSVs.",
            "years": [],
            "categories": [],
        }

    # These can take a few seconds the first time (DuckDB needs to sample the CSVs).
    return {
        "dataset_ok": True,
        "years": list_years(),
        "categories": list_categories(),
    }


@app.get("/api/companies/search")
def companies_search(
    year: str | None = Query(default=None),
    category: str | None = Query(default=None),
    city: str | None = Query(default=None),
    zipcode: str | None = Query(default=None),
    entity_code: str | None = Query(default=None),
    q: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    _: None = Depends(require_api_key),
):
    if not dataset_exists():
        raise HTTPException(status_code=503, detail="Dataset not available on server")

    data = search_companies(
        year=year,
        category=category,
        city=city,
        zipcode=zipcode,
        entity_code=entity_code,
        q=q,
        limit=limit,
        offset=offset,
    )
    return JSONResponse(data)


@app.get("/api/companies/export", response_class=PlainTextResponse)
def companies_export(
    year: str | None = Query(default=None),
    category: str | None = Query(default=None),
    city: str | None = Query(default=None),
    zipcode: str | None = Query(default=None),
    entity_code: str | None = Query(default=None),
    q: str | None = Query(default=None),
    max_rows: int = Query(default=5000, ge=1, le=50000),
    _: None = Depends(require_api_key),
):
    if not dataset_exists():
        raise HTTPException(status_code=503, detail="Dataset not available on server")

    csv_text = export_csv_text(
        year=year,
        category=category,
        city=city,
        zipcode=zipcode,
        entity_code=entity_code,
        q=q,
        max_rows=max_rows,
    )
    return PlainTextResponse(csv_text, media_type="text/csv")


@app.post("/api/leads")
def leads(req: LeadRequest, _: None = Depends(require_api_key)):
    results = run_contacts(
        req.companies_text,
        sleep_between=req.sleep_between,
        per_page_delay_s=req.per_page_delay_s,
        max_companies=req.max_companies,
    )
    return {"count": len(results), "results": results}


@app.post("/api/leads.csv", response_class=PlainTextResponse)
def leads_csv(req: LeadRequest, _: None = Depends(require_api_key)):
    results = run_contacts(
        req.companies_text,
        sleep_between=req.sleep_between,
        per_page_delay_s=req.per_page_delay_s,
        max_companies=req.max_companies,
    )
    csv_text = results_to_csv_text(results)
    return PlainTextResponse(csv_text, media_type="text/csv")