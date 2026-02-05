"""Company contact scraper library.

Adapted from contacts5.py.

Notes:
- This code performs web requests to candidate company sites and crawls a small
  set of pages for emails/phones.
- Respect target sites' robots/ToS and rate-limit as appropriate.
"""

from __future__ import annotations

import csv
import re
import time
from typing import Optional, Iterable
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# Optional: hard overrides when you already know the correct website
WEBSITE_HINTS: dict[str, str] = {
    # Example:
    # "A-B-C Packaging Machine Corp.": "https://www.abcpackaging.com/",
}


def find_emails_in_text(text: str) -> list[str]:
    email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b"
    return list(set(re.findall(email_pattern, text, re.IGNORECASE)))


def clean_company_name(name: str) -> str:
    name = name.strip()

    # Strip anything after commas or obvious parent/company descriptors
    name = re.split(r"\s*,\s*", name, maxsplit=1)[0]
    name = re.split(r"\s+(?:part of|a\s+|an\s+)\b", name, maxsplit=1, flags=re.IGNORECASE)[0]

    # Remove common legal suffixes at end
    name = re.sub(
        r"\b(Inc\.?|LLC|Ltd\.?|Limited|Corporation|Corp\.?|Co\.?|Company|GmbH|S\.p\.A|S\.A\.U\.|S\.r\.l\.?)\b\.?$",
        "",
        name,
        flags=re.IGNORECASE,
    ).strip()

    # Keep letters/numbers/spaces/hyphens; drop other junk
    name = re.sub(r"[^a-zA-Z0-9\s-]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    name = re.sub(r"-{2,}", "-", name)
    name = re.sub(r"\s*-\s*", "-", name)
    return name


def try_common_urls(company_name: str) -> list[str]:
    base = clean_company_name(company_name).lower()
    tokens = [t for t in re.split(r"[\s-]+", base) if t]

    generic_tail = {
        "inc",
        "llc",
        "ltd",
        "limited",
        "corporation",
        "corp",
        "co",
        "company",
        "group",
        "international",
        "systems",
        "technologies",
        "technology",
        "solutions",
        "manufacturing",
        "automation",
        "machinery",
        "machine",
    }

    def strip_generic_tail(ts: list[str]) -> list[str]:
        ts = ts[:]
        while ts and ts[-1] in generic_tail:
            ts.pop()
        return ts

    # Determine acronym prefix (A-B-C style)
    i = 0
    while i < len(tokens) and len(tokens[i]) == 1:
        i += 1
    acronym = "".join(tokens[:i]) if i >= 2 else None
    rest = tokens[i:] if acronym else tokens[:]

    meaningful = [t for t in strip_generic_tail(tokens) if len(t) >= 3 and t not in generic_tail]

    token_lists: list[list[str]] = []
    token_lists.append(tokens)
    stripped = strip_generic_tail(tokens)
    if stripped and stripped != tokens:
        token_lists.append(stripped)

    for cut in (1, 2):
        if len(tokens) > cut:
            tl = tokens[:-cut]
            if meaningful and all(len(x) == 1 for x in tl):
                continue
            token_lists.append(tl)

    if acronym:
        rest_stripped = strip_generic_tail(rest)
        if rest_stripped:
            token_lists.append([acronym] + rest_stripped[:1])
            token_lists.append([acronym] + rest_stripped[:2])
        if not meaningful:
            token_lists.append([acronym])

    candidates: list[str] = []
    for tl in token_lists:
        tl = [x for x in tl if x and x not in generic_tail]
        if not tl:
            continue
        joined = "".join(tl)
        dashed = "-".join(tl)

        if meaningful:
            if acronym and joined == acronym:
                continue
            if all(len(x) == 1 for x in tl):
                continue

        candidates.append(joined)
        candidates.append(dashed)

    seen: set[str] = set()
    cand_out: list[str] = []
    for c in candidates:
        c = c.strip("-")
        if c and c not in seen:
            seen.add(c)
            cand_out.append(c)

    tlds = [".com", ".net", ".org", ".io", ".co", ".us", ".info"]
    schemes = ["https://", "http://"]
    wwws = ["", "www."]

    patterns: list[str] = []
    for dom in cand_out:
        for scheme in schemes:
            for w in wwws:
                for tld in tlds:
                    patterns.append(f"{scheme}{w}{dom}{tld}")
                patterns.append(f"{scheme}{w}{dom}usa.com")
                patterns.append(f"{scheme}{w}{dom}us.com")
                patterns.append(f"{scheme}{w}{dom}-usa.com")
                patterns.append(f"{scheme}{w}{dom}-us.com")

    seen = set()
    out: list[str] = []
    for p in patterns:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out[:220]


def scrape_for_contacts(base_url: str, timeout: int = 10, per_page_delay_s: float = 0.25) -> dict[str, set[str]]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    contacts: dict[str, set[str]] = {"emails": set(), "phones": set()}

    def add_from_html(html: str):
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)

        contacts["emails"].update(find_emails_in_text(text))

        deob = text.replace("[at]", "@").replace("(at)", "@").replace(" at ", "@")
        deob = deob.replace("[dot]", ".").replace("(dot)", ".").replace(" dot ", ".")
        contacts["emails"].update(find_emails_in_text(deob))

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.lower().startswith("mailto:"):
                email = href.split(":", 1)[1].split("?", 1)[0].strip()
                if email:
                    contacts["emails"].add(email)

        phone_pattern = r"(?:\+?1[\s\-.]?)?(?:\(\s*\d{3}\s*\)|\d{3})[\s\-.]?\d{3}[\s\-.]?\d{4}"
        contacts["phones"].update(re.findall(phone_pattern, text))
        return soup

    def fetch(url: str) -> Optional[str]:
        try:
            r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            if r.status_code == 200 and r.text:
                return r.text
        except Exception:
            return None
        return None

    base = base_url.rstrip("/")
    seed_paths = [
        "",
        "contact",
        "contact-us",
        "contacts",
        "about",
        "about-us",
        "company",
        "support",
        "customer-service",
        "help",
        "sales",
        "locations",
        "privacy",
        "impressum",
        "team",
        "leadership",
        "careers",
    ]

    to_visit: list[str] = [urljoin(base + "/", p) for p in seed_paths]
    visited: set[str] = set()

    home_html = fetch(base)
    if home_html:
        soup = add_from_html(home_html)
        keywords = ("contact", "about", "support", "sales", "help", "locations", "team", "privacy", "impressum")
        extra: list[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href or href.startswith("#"):
                continue
            full = urljoin(base + "/", href)
            if full.startswith(base) and any(k in full.lower() for k in keywords):
                extra.append(full)
        to_visit.extend(extra[:10])

    for page in to_visit:
        if page in visited:
            continue
        visited.add(page)
        html = fetch(page)
        if html:
            add_from_html(html)
        if per_page_delay_s:
            time.sleep(per_page_delay_s)

    return contacts


def resolve_url(url: str, timeout: int = 6) -> Optional[str]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        r = requests.head(url, headers=headers, timeout=timeout, allow_redirects=True)
        if r.status_code in (200, 301, 302, 303, 307, 308):
            return r.url.rstrip("/")
        if r.status_code in (403, 405):
            raise Exception("HEAD blocked")
    except Exception:
        pass

    try:
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True, stream=True)
        if r.status_code in (200, 301, 302, 303, 307, 308):
            return r.url.rstrip("/")
    except Exception:
        return None

    return None


def _domain_core(url: str) -> str:
    try:
        netloc = urlparse(url.lower()).netloc
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc.split(".")[0]
    except Exception:
        return ""


def _company_tokens_for_scoring(company_name: str):
    base = clean_company_name(company_name).lower()
    tokens = [t for t in re.split(r"[\s-]+", base) if t]

    generic = {
        "inc",
        "llc",
        "ltd",
        "limited",
        "corporation",
        "corp",
        "co",
        "company",
        "group",
        "international",
        "systems",
        "technologies",
        "technology",
        "solutions",
        "manufacturing",
        "automation",
        "machinery",
        "machine",
    }
    meaningful = [t for t in tokens if len(t) >= 3 and t not in generic]

    i = 0
    while i < len(tokens) and len(tokens[i]) == 1:
        i += 1
    acronym = "".join(tokens[:i]) if i >= 2 else None
    return tokens, meaningful, acronym


def _is_good_domain_for_company(resolved_url: str, meaningful_tokens: list[str], acronym: Optional[str]) -> bool:
    core = _domain_core(resolved_url)
    if not core:
        return False

    if meaningful_tokens:
        if any(t in core for t in meaningful_tokens):
            if acronym and core == acronym:
                return False
            return True
        return False

    return True


def url_priority(u: str, meaningful_tokens: list[str], acronym: Optional[str]) -> int:
    u_l = u.lower()
    score = 0

    if u_l.startswith("https://"):
        score -= 50

    if u_l.endswith(".com") or ".com/" in u_l:
        score -= 40
    elif u_l.endswith(".net") or ".net/" in u_l:
        score -= 10
    elif u_l.endswith(".org") or ".org/" in u_l:
        score -= 5

    if u_l.endswith(".co") or ".co/" in u_l:
        score += 30

    if "://www." in u_l:
        score -= 2

    core = _domain_core(u_l)
    if meaningful_tokens and core:
        matches = sum(1 for t in meaningful_tokens if t in core)
        score -= matches * 25

        if len(core) <= 4:
            score += 80

        if acronym and core == acronym:
            score += 120

    return score


def run_contacts(
    companies: Iterable[str] | str,
    sleep_between: float = 1.0,
    per_page_delay_s: float = 0.25,
    max_companies: int = 100,
) -> list[dict]:
    if isinstance(companies, str):
        company_list = [c.strip() for c in companies.splitlines() if c.strip()]
    else:
        company_list = [str(c).strip() for c in companies if str(c).strip()]

    company_list = company_list[: max(0, int(max_companies))]

    results: list[dict] = []

    for company in company_list:
        result = {"company": company, "website": "", "emails": [], "phones": [], "status": "not found"}

        found_url: Optional[str] = None

        hint = WEBSITE_HINTS.get(company.strip())
        if hint:
            found_url = hint.rstrip("/")
            resolved_hint = resolve_url(found_url)
            if resolved_hint:
                found_url = resolved_hint

        if not found_url:
            urls_to_try = try_common_urls(company)
            _, meaningful_tokens, acronym = _company_tokens_for_scoring(company)

            best_fallback: Optional[str] = None
            for url in sorted(urls_to_try, key=lambda x: url_priority(x, meaningful_tokens, acronym)):
                resolved = resolve_url(url)
                if not resolved:
                    continue
                if _is_good_domain_for_company(resolved, meaningful_tokens, acronym):
                    found_url = resolved
                    break
                if best_fallback is None:
                    best_fallback = resolved
            if not found_url and best_fallback:
                found_url = best_fallback

        if found_url:
            result["website"] = found_url
            result["status"] = "found website"

            contacts = scrape_for_contacts(found_url, per_page_delay_s=per_page_delay_s)
            result["emails"] = sorted(list(contacts["emails"]))
            result["phones"] = sorted(list(contacts["phones"]))

            if result["emails"] or result["phones"]:
                result["status"] = "found contacts"

        results.append(result)

        if sleep_between:
            time.sleep(float(sleep_between))

    return results


def results_to_csv_text(results: list[dict]) -> str:
    import io

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Company", "Website", "Emails", "Phones", "Status"])
    for r in results:
        w.writerow(
            [
                r.get("company", ""),
                r.get("website", ""),
                "; ".join(r.get("emails", []) or []),
                "; ".join(r.get("phones", []) or []),
                r.get("status", ""),
            ]
        )
    return buf.getvalue()