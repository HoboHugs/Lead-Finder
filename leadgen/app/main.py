from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .scraper import run_contacts, results_to_csv_text

app = FastAPI(title="Lead Gen Scraper", version="0.1.0")

app.mount("/static", StaticFiles(directory="static"), name="static")


class LeadRequest(BaseModel):
    companies_text: str = Field(..., description="Company names, one per line")
    sleep_between: float = Field(0.5, ge=0.0, le=5.0)
    per_page_delay_s: float = Field(0.2, ge=0.0, le=2.0)
    max_companies: int = Field(50, ge=1, le=200)


@app.get("/", response_class=HTMLResponse)
def index():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()


@app.post("/api/leads")
def leads(req: LeadRequest):
    results = run_contacts(
        req.companies_text,
        sleep_between=req.sleep_between,
        per_page_delay_s=req.per_page_delay_s,
        max_companies=req.max_companies,
    )
    return {"count": len(results), "results": results}


@app.post("/api/leads.csv", response_class=PlainTextResponse)
def leads_csv(req: LeadRequest):
    results = run_contacts(
        req.companies_text,
        sleep_between=req.sleep_between,
        per_page_delay_s=req.per_page_delay_s,
        max_companies=req.max_companies,
    )
    csv_text = results_to_csv_text(results)
    return PlainTextResponse(csv_text, media_type="text/csv")