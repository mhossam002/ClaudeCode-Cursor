"""
html_processor.py — Fetch and roll-forward an SEC EDGAR HTML filing.

Phase 1 (always):  date/period text substitution (iXBRL numeric tags skipped).
Phase 2 (opt-in):  HTML table blanking + EDGAR comparable-column fill.
                   Activated when edgar_lookup and target_config are supplied.
"""
import logging
import requests
from bs4 import BeautifulSoup, NavigableString

logger = logging.getLogger(__name__)

# Tags whose text content must NOT be substituted
_SKIP_TAGS = {
    "ix:nonfraction", "ix:nonnumeric", "ix:header", "ix:hidden",
    "script", "style",
}


def fetch_filing_html(url: str, user_agent: str) -> str:
    """GET raw HTML from EDGAR. Raises requests.HTTPError on failure."""
    resp = requests.get(
        url,
        headers={"User-Agent": user_agent, "Accept": "text/html,application/xhtml+xml"},
        timeout=60,
    )
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


def transform_html_text(html: str, rules: list) -> tuple:
    """
    Parse *html*, replace text nodes that are NOT inside iXBRL/script/style
    ancestors, inject <base href> for relative assets.

    Returns (transformed_html_string, replacement_count).
    """
    soup = BeautifulSoup(html, "lxml")

    # Inject base tag so relative CSS/image URLs resolve on SEC's servers
    head = soup.find("head")
    if head and not soup.find("base"):
        base = soup.new_tag("base", href="https://www.sec.gov/")
        head.insert(0, base)

    count = 0
    for node in soup.find_all(string=True):
        if not isinstance(node, NavigableString) or not node.strip():
            continue
        # Skip if any ancestor is a forbidden tag
        if any(p.name and p.name.lower() in _SKIP_TAGS for p in node.parents):
            continue
        new_text = str(node)
        for old, new in rules:
            if old in new_text:
                new_text = new_text.replace(old, new)
                count += 1
        if new_text != str(node):
            node.replace_with(NavigableString(new_text))

    return str(soup), count


def process_filing_html(cik, accession_number, primary_document,
                        rules, user_agent, progress_callback=None,
                        edgar_lookup=None, target_config=None) -> dict:
    """
    Orchestrate: build URL → fetch → text-transform → [table pass] → result dict.

    Parameters
    ----------
    edgar_lookup  : if provided (from build_fact_lookup), enables Phase 2 table
                    blanking and comparable-period EDGAR fill.
    target_config : required when edgar_lookup is supplied; supplies period labels
                    and comparable_end for column identification.

    Returns dict with keys:
        status, html, url, replacements, error,
        tables_processed, cells_blanked, edgar_inserted, edgar_missing
    """
    from rollforward.edgar_client import build_filing_url

    def emit(msg):
        logger.info(msg)
        if progress_callback:
            progress_callback(msg)

    url = build_filing_url(cik, accession_number, primary_document)
    emit(f"Fetching filing HTML: {url}")
    try:
        raw_html = fetch_filing_html(url, user_agent)
    except Exception as exc:
        return {"status": "error", "html": "", "url": url,
                "replacements": 0, "error": str(exc),
                "tables_processed": 0, "cells_blanked": 0,
                "edgar_inserted": 0, "edgar_missing": []}

    emit(f"Fetched {len(raw_html):,} bytes — applying {len(rules)} substitution rules…")
    modified_html, count = transform_html_text(raw_html, rules)
    emit(f"Text pass complete — {count} replacements")

    # ── Phase 2: table blanking + EDGAR fill ──────────────────────────────
    table_stats = {
        "tables_processed": 0, "cells_blanked": 0,
        "edgar_inserted": 0, "edgar_missing": [],
    }
    if edgar_lookup is not None and target_config:
        try:
            from rollforward.html_table_processor import process_html_tables
            emit("Applying table roll-forward (blanking current-period + EDGAR fill)…")
            soup = BeautifulSoup(modified_html, "lxml")
            table_stats = process_html_tables(soup, target_config, edgar_lookup)
            modified_html = str(soup)
            emit(
                f"Table pass complete — {table_stats['tables_processed']} tables, "
                f"{table_stats['cells_blanked']} cells blanked, "
                f"{table_stats['edgar_inserted']} EDGAR values inserted"
            )
        except Exception as exc:
            logger.exception("HTML table pass failed")
            emit(f"Warning: table pass failed ({exc}); text-only output returned")

    return {
        "status":     "ok",
        "html":       modified_html,
        "url":        url,
        "replacements": count,
        "error":      None,
        **table_stats,
    }
