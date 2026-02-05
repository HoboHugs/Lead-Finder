#!/usr/bin/env python3
"""
Improved Company Contact Scraper
Finds company websites and extracts email contacts
"""

import requests
from bs4 import BeautifulSoup
import re
import csv
import time
from urllib.parse import urljoin, urlparse
import json

# Your company list
COMPANIES = """A-B-C Packaging Machine Corp.
AAF International
ACO, Inc
ACSIS, part of Antares Vision Group
ADCO Manufacturing, a Massman Company
AFA Systems Ltd.
AIM, Inc.
AMERISCEND

"""

# Optional: hard overrides when you already know the correct website
WEBSITE_HINTS = {
    # Provided example:
    "A-B-C Packaging Machine Corp.": "https://www.abcpackaging.com/",
}


def find_emails_in_text(text):
    """Extract emails from text"""
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
    return list(set(re.findall(email_pattern, text, re.IGNORECASE)))


def clean_company_name(name):
    """Clean company name for URL search (preserve info needed for domain variants)."""
    name = name.strip()

    # Strip anything after commas or obvious parent/company descriptors
    name = re.split(r'\s*,\s*', name, maxsplit=1)[0]
    name = re.split(r'\s+(?:part of|a\s+|an\s+)\b', name, maxsplit=1, flags=re.IGNORECASE)[0]

    # Remove common legal suffixes at end
    name = re.sub(
        r'\b(Inc\.?|LLC|Ltd\.?|Limited|Corporation|Corp\.?|Co\.?|Company|GmbH|S\.p\.A|S\.A\.U\.|S\.r\.l\.?)\b\.?$',
        '',
        name,
        flags=re.IGNORECASE
    ).strip()

    # Keep letters/numbers/spaces/hyphens; drop other junk
    name = re.sub(r'[^a-zA-Z0-9\s-]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    name = re.sub(r'-{2,}', '-', name)
    name = re.sub(r'\s*-\s*', '-', name)  # "A - B" -> "A-B"
    return name


def try_common_urls(company_name):
    """Try many URL/domain variations, but avoid acronym-only collisions (abc.com, etc.)."""
    base = clean_company_name(company_name).lower()
    tokens = [t for t in re.split(r'[\s-]+', base) if t]

    generic_tail = {
        "inc", "llc", "ltd", "limited", "corporation", "corp", "co", "company",
        "group", "international", "systems", "technologies", "technology",
        "solutions", "manufacturing", "automation", "machinery", "machine"
    }

    def strip_generic_tail(ts):
        ts = ts[:]
        while ts and ts[-1] in generic_tail:
            ts.pop()
        return ts

    # Determine acronym prefix (A-B-C style)
    i = 0
    while i < len(tokens) and len(tokens[i]) == 1:
        i += 1
    acronym = ''.join(tokens[:i]) if i >= 2 else None
    rest = tokens[i:] if acronym else tokens[:]

    # Meaningful words (used to avoid abc.com style collisions)
    meaningful = [t for t in strip_generic_tail(tokens) if len(t) >= 3 and t not in generic_tail]

    token_lists = []

    # Full + tail stripped
    token_lists.append(tokens)
    stripped = strip_generic_tail(tokens)
    if stripped and stripped != tokens:
        token_lists.append(stripped)

    # Truncations, but DO NOT truncate to acronym-only if there are meaningful trailing words
    for cut in (1, 2):
        if len(tokens) > cut:
            tl = tokens[:-cut]
            # If tl is only single-letter tokens and original had meaningful words, skip
            if meaningful and all(len(x) == 1 for x in tl):
                continue
            token_lists.append(tl)

    # Acronym variants: require at least 1 meaningful rest token if rest exists
    if acronym:
        rest_stripped = strip_generic_tail(rest)

        if rest_stripped:
            # abc + packaging (best for your ABC Packaging case)
            token_lists.append([acronym] + rest_stripped[:1])
            # abc + packaging + machine (if you want deeper)
            token_lists.append([acronym] + rest_stripped[:2])

        # DO NOT add [acronym] alone unless the entire company name is basically just that
        # (i.e., there are no meaningful words)
        if not meaningful:
            token_lists.append([acronym])

    # Build domain candidates from token lists
    candidates = []
    for tl in token_lists:
        tl = [x for x in tl if x and x not in generic_tail]
        if not tl:
            continue

        joined = ''.join(tl)
        dashed = '-'.join(tl)

        # Guardrail: avoid acronym-only collisions (abc.com) but allow short legit brands (aco.com, aim.com)
        if meaningful:
            if acronym and joined == acronym:
                continue
            # If this candidate is basically only collapsed single-letter tokens (e.g., "abc") and we have meaningful words elsewhere, skip it
            if all(len(x) == 1 for x in tl):
                continue

        candidates.append(joined)
        candidates.append(dashed)

    # De-dupe preserving order
    seen = set()
    cand_out = []
    for c in candidates:
        c = c.strip('-')
        if c and c not in seen:
            seen.add(c)
            cand_out.append(c)

    tlds = [".com", ".net", ".org", ".io", ".co", ".us", ".info"]
    schemes = ["https://", "http://"]
    wwws = ["", "www."]

    patterns = []
    for dom in cand_out:
        for scheme in schemes:
            for w in wwws:
                for tld in tlds:
                    patterns.append(f"{scheme}{w}{dom}{tld}")
                patterns.append(f"{scheme}{w}{dom}usa.com")
                patterns.append(f"{scheme}{w}{dom}us.com")
                patterns.append(f"{scheme}{w}{dom}-usa.com")
                patterns.append(f"{scheme}{w}{dom}-us.com")

    # De-dupe + bound runtime
    seen = set()
    out = []
    for p in patterns:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out[:220]


def scrape_for_contacts(base_url, timeout=10):
    """Scrape a website for contact information (emails + phones)."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    contacts = {'emails': set(), 'phones': set()}

    def add_from_html(html):
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text(" ", strip=True)

        # Emails in visible text
        contacts['emails'].update(find_emails_in_text(text))

        # Common obfuscations: "name [at] domain [dot] com"
        deob = text.replace("[at]", "@").replace("(at)", "@").replace(" at ", "@")
        deob = deob.replace("[dot]", ".").replace("(dot)", ".").replace(" dot ", ".")
        contacts['emails'].update(find_emails_in_text(deob))

        # mailto links
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.lower().startswith('mailto:'):
                email = href.split(':', 1)[1].split('?', 1)[0].strip()
                if email:
                    contacts['emails'].add(email)

        # Phones (looser, but still sane)
        phone_pattern = r'(?:\+?1[\s\-.]?)?(?:\(\s*\d{3}\s*\)|\d{3})[\s\-.]?\d{3}[\s\-.]?\d{4}'
        contacts['phones'].update(re.findall(phone_pattern, text))

        return soup

    def fetch(url):
        try:
            r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            if r.status_code == 200 and r.text:
                return r.text
        except Exception:
            return None
        return None

    base = base_url.rstrip('/')

    # Seed pages (built correctly with urljoin)
    seed_paths = [
        "", "contact", "contact-us", "contacts",
        "about", "about-us", "company",
        "support", "customer-service", "help",
        "sales", "locations", "privacy", "impressum",
        "team", "leadership", "careers"
    ]

    to_visit = []
    for p in seed_paths:
        to_visit.append(urljoin(base + "/", p))

    visited = set()

    # 1) Fetch homepage first and extract a few “contact-like” internal links
    home_html = fetch(base)
    if home_html:
        soup = add_from_html(home_html)

        keywords = ("contact", "about", "support", "sales", "help", "locations", "team", "privacy", "impressum")
        extra = []
        for a in soup.find_all('a', href=True):
            href = a['href'].strip()
            if not href or href.startswith('#'):
                continue
            full = urljoin(base + "/", href)
            if full.startswith(base) and any(k in full.lower() for k in keywords):
                extra.append(full)

        # add up to 10 extra internal pages
        for u in extra[:10]:
            to_visit.append(u)

    # 2) Crawl a bounded list of pages
    for page in to_visit:
        if page in visited:
            continue
        visited.add(page)

        html = fetch(page)
        if html:
            add_from_html(html)

        time.sleep(0.25)

    return contacts


def resolve_url(url, timeout=6):
    """
    Return the final canonical URL if reachable, else None.
    Prefers resolving redirects so we scrape the real site.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    # Try HEAD first
    try:
        r = requests.head(url, headers=headers, timeout=timeout, allow_redirects=True)
        if r.status_code in (200, 301, 302, 303, 307, 308):
            return r.url.rstrip('/')
        if r.status_code in (403, 405):
            raise Exception("HEAD blocked")
    except Exception:
        pass

    # Fallback to GET
    try:
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True, stream=True)
        if r.status_code in (200, 301, 302, 303, 307, 308):
            return r.url.rstrip('/')
    except Exception:
        return None

    return None

def _domain_core(url: str) -> str:
    """Return the first label of the host (no www), lowercased."""
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
        "inc", "llc", "ltd", "limited", "corporation", "corp", "co", "company",
        "group", "international", "systems", "technologies", "technology",
        "solutions", "manufacturing", "automation", "machinery", "machine"
    }
    meaningful = [t for t in tokens if len(t) >= 3 and t not in generic]

    # Acronym prefix (A-B-C style)
    i = 0
    while i < len(tokens) and len(tokens[i]) == 1:
        i += 1
    acronym = "".join(tokens[:i]) if i >= 2 else None

    return tokens, meaningful, acronym

from typing import Optional

def _is_good_domain_for_company(resolved_url: str, meaningful_tokens: list, acronym: Optional[str]) -> bool:
    """Reject obvious collisions like abc.com when we expect 'packaging' etc."""
    core = _domain_core(resolved_url)
    if not core:
        return False

    if meaningful_tokens:
        # Must contain at least one meaningful token somewhere in the core
        if any(t in core for t in meaningful_tokens):
            # Also avoid exact acronym-only cores
            if acronym and core == acronym:
                return False
            return True
        return False

    # If we don't have meaningful tokens, accept
    return True


def url_priority(u: str, meaningful_tokens: list, acronym: Optional[str]) -> int:
    """Lower score = tried earlier."""
    u_l = u.lower()
    score = 0

    # Prefer https
    if u_l.startswith("https://"):
        score -= 50

    # Prefer .com then .net then .org etc (by ending, not substring)
    if u_l.endswith(".com") or ".com/" in u_l:
        score -= 40
    elif u_l.endswith(".net") or ".net/" in u_l:
        score -= 10
    elif u_l.endswith(".org") or ".org/" in u_l:
        score -= 5

    # Penalize .co a bit (lots of collisions)
    if u_l.endswith(".co") or ".co/" in u_l:
        score += 30

    # Slight preference for www
    if "://www." in u_l:
        score -= 2

    # Token matching in the domain core
    core = _domain_core(u_l)
    matches = 0
    if meaningful_tokens and core:
        matches = sum(1 for t in meaningful_tokens if t in core)
        score -= matches * 25

        # Strong penalty for collision-prone short cores when we have meaningful words
        if len(core) <= 4:
            score += 80

        # Penalize exact acronym core
        if acronym and core == acronym:
            score += 120

    return score

def run_contacts(companies, out_csv_path="packexpo_contacts.csv", sleep_between=1.0):
    """
    Run the contact scrape for a list of company names.

    companies: list[str] (or newline-separated string)
    out_csv_path: where to write the CSV
    sleep_between: seconds to pause between companies (be respectful)
    Returns: out_csv_path
    """
    if isinstance(companies, str):
        companies = [c.strip() for c in companies.splitlines() if c.strip()]
    else:
        companies = [str(c).strip() for c in companies if str(c).strip()]

    results = []

    print(f"Starting to process {len(companies)} companies...")
    print("This will take a while. Please be patient.\n")

    for i, company in enumerate(companies, 1):
        print(f"[{i}/{len(companies)}] {company}")

        result = {
            'company': company,
            'website': '',
            'emails': [],
            'phones': [],
            'status': 'not found'
        }

        found_url = None

        # 0) Hard hint override
        hint = WEBSITE_HINTS.get(company.strip())
        if hint:
            found_url = hint.rstrip('/')
            resolved_hint = resolve_url(found_url)
            if resolved_hint:
                found_url = resolved_hint

        # 1) Otherwise, try generated candidates
        if not found_url:
            urls_to_try = try_common_urls(company)
            _, meaningful_tokens, acronym = _company_tokens_for_scoring(company)

            best_fallback = None
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
            result['website'] = found_url
            result['status'] = 'found website'
            print(f"  ✓ Found: {found_url}")

            contacts = scrape_for_contacts(found_url)
            result['emails'] = list(contacts['emails'])
            result['phones'] = list(contacts['phones'])

            if result['emails']:
                result['status'] = 'found contacts'
                print(f"  ✓ Emails: {', '.join(result['emails'][:3])}")
            if result['phones']:
                print(f"  ✓ Phones: {', '.join(result['phones'][:2])}")
        else:
            print(f"  ✗ No website found")

        results.append(result)

        # Respectful delay
        if sleep_between:
            time.sleep(float(sleep_between))

    # Save results
    with open(out_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Company', 'Website', 'Emails', 'Phones', 'Status'])

        for r in results:
            writer.writerow([
                r['company'],
                r['website'],
                '; '.join(r['emails']),
                '; '.join(r['phones']),
                r['status']
            ])

    print(f"\nResults saved to: {out_csv_path}")
    return out_csv_path


def main():
    companies = [c.strip() for c in COMPANIES.split('\n') if c.strip()]
    run_contacts(companies, out_csv_path="packexpo_contacts.csv", sleep_between=1.0)


if __name__ == "__main__":
    main()
