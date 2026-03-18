"""
edgar_client.py — SEC EDGAR XBRL API fetch, fact lookup, and filing detection.
"""
import calendar
import logging
import requests
from datetime import date, datetime

logger = logging.getLogger(__name__)

EDGAR_FACTS_URL       = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

# ---------------------------------------------------------------------------
# Ticker → CIK resolution
# ---------------------------------------------------------------------------

_TICKERS_CACHE: dict = {}   # {ticker_upper: {cik, name, ticker}}


def lookup_ticker(ticker: str, user_agent: str) -> dict:
    """Resolve a ticker symbol to CIK + company name via SEC's company_tickers.json.

    Returns: {"cik": "0000783412", "name": "Daily Journal Corp", "ticker": "DJCO"}
    Raises:  ValueError if ticker not found.
    """
    global _TICKERS_CACHE
    key = ticker.strip().upper()
    if not _TICKERS_CACHE:
        resp = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers={"User-Agent": user_agent},
            timeout=10,
        )
        resp.raise_for_status()
        for entry in resp.json().values():
            _TICKERS_CACHE[entry["ticker"].upper()] = {
                "cik":    str(entry["cik_str"]).zfill(10),
                "name":   entry["title"],
                "ticker": entry["ticker"],
            }
    if key not in _TICKERS_CACHE:
        raise ValueError(f"Ticker '{ticker}' not found in SEC registry")
    return _TICKERS_CACHE[key]
EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"


def build_filing_url(cik: str, accession_number: str, primary_document: str) -> str:
    """Construct the EDGAR Archives URL for the primary HTML document of a filing."""
    cik_int    = int(cik)                         # strips leading zeros
    acc_nodash = accession_number.replace("-", "")
    return (f"https://www.sec.gov/Archives/edgar/data"
            f"/{cik_int}/{acc_nodash}/{primary_document}")

# Approximate month ranges for duration-based facts
MONTH_RANGES = {
    3:  (85,  95),    # ~3 months
    6:  (178, 187),   # ~6 months
    9:  (270, 280),   # ~9 months
    12: (360, 370),   # ~12 months (annual)
}

_ORDINALS = {1: "first", 2: "second", 3: "third", 4: "fourth"}


# ---------------------------------------------------------------------------
# Period auto-detection
# ---------------------------------------------------------------------------

def _format_label(d: date) -> str:
    """'December 31, 2025' from a date object."""
    return f"{d.strftime('%B')} {d.day}, {d.year}"


def _last_day_of_month(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def _add_months(d: date, months: int) -> date:
    """Add N months to a date and return the last day of the resulting month."""
    total = d.month + months
    year  = d.year + (total - 1) // 12
    month = (total - 1) % 12 + 1
    return _last_day_of_month(year, month)


def _add_years(d: date, years: int = 1) -> date:
    """Add N years, clamping to last valid day (handles Feb 28/29)."""
    try:
        return d.replace(year=d.year + years)
    except ValueError:
        return _last_day_of_month(d.year + years, d.month)


def _subtract_years(d: date, years: int = 1) -> date:
    """Subtract N years, clamping to last valid day (handles Feb 28/29)."""
    try:
        return d.replace(year=d.year - years)
    except ValueError:
        return _last_day_of_month(d.year - years, d.month)


def _most_recent_fy_end(period: date, fy_month: int, fy_day: int) -> date:
    """
    Return the fiscal year-end date that most recently preceded `period`.
    E.g. for DJCO (Sep 30) and period Dec 31 2025 → Sep 30 2025.
    """
    candidate = _last_day_of_month(period.year, fy_month)
    # Make sure fy_day is honored (e.g. Sep 30 not Sep 31)
    candidate = date(period.year, fy_month, min(fy_day, calendar.monthrange(period.year, fy_month)[1]))
    if candidate >= period:
        candidate = date(period.year - 1, fy_month, min(fy_day, calendar.monthrange(period.year - 1, fy_month)[1]))
    return candidate


def _months_after_fy_end(period: date, fy_end_month: int) -> int:
    """Return how many months after the fiscal year end the period falls (3, 6, or 9)."""
    diff = (period.month - fy_end_month) % 12
    return diff if diff > 0 else 12


def fetch_submissions(cik: str, user_agent: str) -> dict:
    """GET the EDGAR submissions JSON for a CIK."""
    url = EDGAR_SUBMISSIONS_URL.format(cik=cik.zfill(10))
    headers = {"User-Agent": user_agent, "Accept": "application/json"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def list_available_filings(cik: str, user_agent: str,
                           form_types=("10-Q", "10-K", "10-K405"),
                           limit: int = 20) -> list:
    """
    Return a list of recent filings for *cik* filtered by form_types.

    Each item: {
        "form": "10-Q",
        "period_end": "2025-12-31",
        "filing_date": "2026-02-14",
        "accession_number": "0000783412-26-000010",
        "primary_document": "djco-20251231.htm",
    }
    """
    data    = fetch_submissions(cik, user_agent)
    filings = data.get("filings", {}).get("recent", {})
    forms        = filings.get("form", [])
    report_dates = filings.get("reportDate", [])
    filing_dates = filings.get("filingDate", [])
    accessions   = filings.get("accessionNumber", [])
    primary_docs = filings.get("primaryDocument", [])

    result = []
    for i, form in enumerate(forms):
        if form in form_types:
            result.append({
                "form":             form,
                "period_end":       report_dates[i] if i < len(report_dates) else "",
                "filing_date":      filing_dates[i] if i < len(filing_dates) else "",
                "accession_number": accessions[i]   if i < len(accessions)   else "",
                "primary_document": primary_docs[i] if i < len(primary_docs) else "",
            })
            if len(result) >= limit:
                break
    return result


def detect_annual_config(cik: str, user_agent: str) -> dict:
    """
    Inspect the EDGAR submissions feed for *cik* and derive complete
    source and target period configs for a 10-K roll-forward.

    For a 10-K, the "source" is the most recent 10-K filing and the
    "target" is the next fiscal year end.

    Returns same structure as detect_period_config() but with form="10-K"
    in filing_info and appropriate annual period configs.
    """
    data    = fetch_submissions(cik, user_agent)
    filings = data.get("filings", {}).get("recent", {})
    forms         = filings.get("form", [])
    report_dates  = filings.get("reportDate", [])
    filing_dates  = filings.get("filingDate", [])
    accessions    = filings.get("accessionNumber", [])
    primary_docs  = filings.get("primaryDocument", [])

    # Find most recent 10-K (or 10-K405)
    latest_10k = None
    for i, form in enumerate(forms):
        if form in ("10-K", "10-K405") and latest_10k is None:
            latest_10k = {
                "period_end":       report_dates[i],
                "filing_date":      filing_dates[i],
                "accession_number": accessions[i],
                "form":             form,
                "primary_document": primary_docs[i] if i < len(primary_docs) else "",
            }
            break

    if not latest_10k:
        raise ValueError(f"No 10-K filing found for CIK {cik}")

    # Parse source (most recent 10-K) dates
    src_period = datetime.strptime(latest_10k["period_end"],  "%Y-%m-%d").date()
    src_filed  = datetime.strptime(latest_10k["filing_date"], "%Y-%m-%d").date()

    # Target = one year after source (same fiscal year-end, next year)
    tgt_period = _add_years(src_period, 1)

    # Comparable end for 10-K = one year prior to source (i.e. the year before source FY end)
    # For presentation: prior year column = source period end (one year before target)
    src_comparable = _subtract_years(src_period, 1)

    # Target comparable = source period (prior year relative to target)
    tgt_comparable = src_period

    # prior_year_end_label for annual = the source period label
    # (the FY end IS the prior year end for the target)
    prior_ye_label = _format_label(src_period)

    source_config = {
        "period_end":           src_period.isoformat(),
        "period_label":         _format_label(src_period),
        "comparable_end":       src_comparable.isoformat(),
        "comparable_label":     _format_label(src_comparable),
        "quarter_name":         "annual",
        "filing_date":          _format_label(src_filed),
        "ytd_months":           12,
        "prior_year_end_label": _format_label(src_period),
    }
    target_config = {
        "period_end":           tgt_period.isoformat(),
        "period_label":         _format_label(tgt_period),
        "comparable_end":       tgt_comparable.isoformat(),
        "comparable_label":     _format_label(tgt_comparable),
        "quarter_name":         "annual",
        "filing_date":          "[FILING DATE]",
        "ytd_months":           12,
        "prior_year_end_label": prior_ye_label,
    }

    return {
        "source": source_config,
        "target": target_config,
        "filing_info": {
            "form":             latest_10k["form"],
            "period_end":       latest_10k["period_end"],
            "filing_date":      latest_10k["filing_date"],
            "accession_number": latest_10k["accession_number"],
            "primary_document": latest_10k.get("primary_document", ""),
            "company_name":     data.get("name", ""),
            "fy_end_label":     _format_label(src_period),
        },
        "warnings": [],
    }


def detect_period_config(cik: str, user_agent: str, form_type: str = "10-Q") -> dict:
    """
    Inspect the EDGAR submissions feed for *cik* and derive complete
    source and target period configuration dicts for a roll-forward.

    Parameters
    ----------
    cik        : SEC CIK (numeric string, zero-padding applied automatically).
    user_agent : Required by SEC (e.g. "Company Name email@example.com").
    form_type  : "10-Q" (default) or "10-K". When "10-K", delegates to
                 detect_annual_config() and returns its result unchanged.

    Returns:
    {
        "source": { period_end, period_label, comparable_end, comparable_label,
                    quarter_name, filing_date, ytd_months, prior_year_end_label },
        "target": { … same keys … },
        "filing_info": { form, period_end, filing_date, accession_number,
                         company_name, fy_end_label }
    }
    Raises ValueError if no matching filing is found for this CIK.
    """
    # Delegate annual filings to the dedicated function
    if form_type == "10-K":
        return detect_annual_config(cik, user_agent)

    data     = fetch_submissions(cik, user_agent)
    filings  = data.get("filings", {}).get("recent", {})
    forms    = filings.get("form", [])
    report_dates  = filings.get("reportDate", [])
    filing_dates  = filings.get("filingDate", [])
    accessions    = filings.get("accessionNumber", [])
    primary_docs  = filings.get("primaryDocument", [])

    # Find most recent 10-Q and most recent 10-K (for fiscal year end)
    latest_10q = latest_10k = None
    for i, form in enumerate(forms):
        if form == "10-Q" and latest_10q is None:
            latest_10q = {
                "period_end":       report_dates[i],
                "filing_date":      filing_dates[i],
                "accession_number": accessions[i],
                "primary_document": primary_docs[i] if i < len(primary_docs) else "",
            }
        if form in ("10-K", "10-K405") and latest_10k is None:
            latest_10k = {
                "period_end":       report_dates[i],
                "filing_date":      filing_dates[i],
                "accession_number": accessions[i],
            }
        if latest_10q and latest_10k:
            break

    if not latest_10q:
        raise ValueError(f"No 10-Q filing found for CIK {cik}")

    # Parse key dates
    src_period   = datetime.strptime(latest_10q["period_end"],  "%Y-%m-%d").date()
    src_filed    = datetime.strptime(latest_10q["filing_date"], "%Y-%m-%d").date()

    # Fiscal year end from the latest 10-K (default Sep 30 if no 10-K found)
    if latest_10k:
        fy_date  = datetime.strptime(latest_10k["period_end"], "%Y-%m-%d").date()
        fy_month = fy_date.month
        fy_day   = fy_date.day
    else:
        fy_month, fy_day = 9, 30   # DJCO default
        fy_date  = _most_recent_fy_end(src_period, fy_month, fy_day)

    # Derive quarter ordinal and YTD months
    ytd_months   = _months_after_fy_end(src_period, fy_month)
    quarter_num  = ytd_months // 3
    quarter_name = _ORDINALS.get(quarter_num, f"quarter-{quarter_num}")

    # Comparable period (same quarter, one year earlier)
    src_comparable = _subtract_years(src_period, 1)

    # Prior year-end for balance sheet (fiscal year end immediately before source period)
    prior_ye_date  = _most_recent_fy_end(src_period, fy_month, fy_day)

    # Target period (next quarter)
    tgt_period     = _add_months(src_period, 3)
    tgt_comparable = _subtract_years(tgt_period, 1)
    tgt_ytd_months = ytd_months + 3
    tgt_quarter_num  = tgt_ytd_months // 3
    tgt_quarter_name = _ORDINALS.get(tgt_quarter_num, f"quarter-{tgt_quarter_num}")

    # For Q3→ the "next period" would be the fiscal year end (annual 10-K)
    # so we still compute it but warn
    warnings = []
    if tgt_ytd_months >= 12:
        warnings.append(
            f"Target period ({_format_label(tgt_period)}) is a full fiscal year — "
            "this roll-forward is typically not used for annual 10-K filings."
        )

    source_config = {
        "period_end":           src_period.isoformat(),
        "period_label":         _format_label(src_period),
        "comparable_end":       src_comparable.isoformat(),
        "comparable_label":     _format_label(src_comparable),
        "quarter_name":         quarter_name,
        "filing_date":          _format_label(src_filed),
        "ytd_months":           ytd_months,
        "prior_year_end_label": _format_label(prior_ye_date),
    }
    target_config = {
        "period_end":           tgt_period.isoformat(),
        "period_label":         _format_label(tgt_period),
        "comparable_end":       tgt_comparable.isoformat(),
        "comparable_label":     _format_label(tgt_comparable),
        "quarter_name":         tgt_quarter_name,
        "filing_date":          "[FILING DATE]",
        "ytd_months":           tgt_ytd_months,
        "prior_year_end_label": _format_label(prior_ye_date),   # unchanged
    }

    return {
        "source": source_config,
        "target": target_config,
        "filing_info": {
            "form":             "10-Q",
            "period_end":       latest_10q["period_end"],
            "filing_date":      latest_10q["filing_date"],
            "accession_number": latest_10q["accession_number"],
            "primary_document": latest_10q.get("primary_document", ""),
            "company_name":     data.get("name", ""),
            "fy_end_label":     _format_label(fy_date),
        },
        "warnings": warnings,
    }


def fetch_company_facts(cik: str, user_agent: str) -> dict:
    """
    GET https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json
    user_agent is required by SEC (e.g. "Company Name email@example.com")
    Returns the parsed JSON dict or raises on HTTP error.
    """
    url = EDGAR_FACTS_URL.format(cik=cik.zfill(10))
    headers = {"User-Agent": user_agent, "Accept": "application/json"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _days_between(start: str, end: str) -> int:
    return (_parse_date(end) - _parse_date(start)).days


def build_fact_lookup(facts_json: dict) -> dict:
    """
    Build a flat lookup dict:
        (concept_name, end_date_str, approx_months) -> value

    Handles:
    - Duration facts: have 'start' and 'end'; approx_months is 3, 6, 9, or 12
    - Instant facts: no 'start'; stored as approx_months=0
    Searches both us-gaap and any extension namespace (e.g. djco).
    """
    lookup = {}
    taxonomy_dicts = facts_json.get("facts", {})

    for taxonomy, concepts in taxonomy_dicts.items():
        for concept, concept_data in concepts.items():
            # Strip namespace prefix for clean key
            short_concept = concept.split(":")[-1] if ":" in concept else concept
            units_map = concept_data.get("units", {})

            for unit_key, filings in units_map.items():
                for filing in filings:
                    end = filing.get("end")
                    start = filing.get("start")
                    val = filing.get("val")
                    if end is None or val is None:
                        continue

                    if start is not None:
                        # Duration fact — classify by approximate months
                        days = _days_between(start, end)
                        matched_months = None
                        for months, (lo, hi) in MONTH_RANGES.items():
                            if lo <= days <= hi:
                                matched_months = months
                                break
                        if matched_months is None:
                            continue  # skip unrecognised durations
                        key = (short_concept, end, matched_months)
                    else:
                        # Instant fact
                        key = (short_concept, end, 0)

                    # Prefer the value from the most recent filing form
                    # (later filings may amend; keep last seen)
                    lookup[key] = val

    return lookup


def get_fact(lookup: dict, concept: str, period_end: str, months: int):
    """
    Convenience accessor. Returns value or None if not found.
    months=0 means instant fact.
    """
    key = (concept, period_end, months)
    val = lookup.get(key)
    if val is None:
        logger.warning("EDGAR fact not found: concept=%s period_end=%s months=%s", concept, period_end, months)
    return val
