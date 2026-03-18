"""
html_table_processor.py — HTML financial table blanking + EDGAR value insertion.

Operates on a BeautifulSoup tree (parsed with lxml) produced by html_processor.py
after the text-substitution pass has already been applied.  At that point:

  • The current-period column header says the TARGET period label
    (e.g. "March 31, 2026") because text substitution renamed it.
  • The comparable-period column header says the TARGET comparable label
    (e.g. "March 31, 2025") for the same reason.

We use those post-substitution labels to locate the right columns.
"""

import re
import logging
from bs4 import NavigableString

from .concept_row_map import INCOME_STATEMENT_MAP, CASH_FLOW_MAP, get_format_hint

logger = logging.getLogger(__name__)

# ── Numeric-detection pattern ──────────────────────────────────────────────
_NUMERIC_RE = re.compile(r'^[\s\-–—\(\)\$,\d\.]+$')


def _cell_text(cell) -> str:
    """Plain text of a cell, whitespace-stripped."""
    return cell.get_text(separator=" ", strip=True)


def _is_numeric(text: str) -> bool:
    """True if text looks like a financial number (could be blank/dash)."""
    cleaned = text.replace(",", "").replace("(", "-").replace(")", "").replace("$", "").strip()
    if cleaned in ("", "—", "–", "-"):
        return False
    try:
        float(cleaned)
        return bool(re.search(r"\d", cleaned))
    except ValueError:
        return False


def _blank_cell(cell) -> bool:
    """
    Remove iXBRL wrappers and clear numeric text from a BeautifulSoup cell element.
    Returns True if the cell was actually cleared.
    """
    # Unwrap iXBRL numeric/non-numeric tags so only text nodes remain
    for tag in cell.find_all(["ix:nonfraction", "ix:nonnumeric"]):
        tag.unwrap()

    text = _cell_text(cell)
    if not _is_numeric(text):
        return False

    # Clear all text nodes inside the cell
    for node in list(cell.strings):
        node.replace_with(NavigableString(""))
    return True


def _set_cell_text(cell, value: str):
    """Replace the entire content of a cell with plain text."""
    for child in list(cell.children):
        child.extract()
    cell.append(NavigableString(value))


def _format_value(value, format_hint: str) -> str:
    """Format an EDGAR numeric value for HTML display."""
    if value is None:
        return ""
    try:
        v = float(value)
        if format_hint == "per_share":
            return f"{v:.2f}"
        elif format_hint == "shares":
            return f"{int(round(v)):,}"
        else:  # currency (in thousands)
            if v < 0:
                return f"({int(round(abs(v))):,})"
            return f"{int(round(v)):,}"
    except (ValueError, TypeError):
        return str(value)


def _header_col_map(header_row) -> dict:
    """
    Map visual column index → cell element for a header row, accounting
    for colspan so that tables with merged headers are handled correctly.

    Returns {visual_col_index: cell_element}.
    """
    result = {}
    col = 0
    for cell in header_row.find_all(["th", "td"]):
        span = int(cell.get("colspan", 1))
        for i in range(span):
            result[col + i] = cell
        col += span
    return result


def _row_cells_by_col(row) -> dict:
    """
    Map visual column index → cell element for a data row, accounting for colspan.
    """
    result = {}
    col = 0
    for cell in row.find_all(["td", "th"]):
        span = int(cell.get("colspan", 1))
        for i in range(span):
            result[col + i] = cell
        col += span
    return result


def _infer_months(header_text: str, default: int) -> int:
    """Infer the YTD month count from a column header string."""
    lower = header_text.lower()
    if "nine months" in lower:
        return 9
    if "six months" in lower:
        return 6
    if "three months" in lower:
        return 3
    if "year ended" in lower or "twelve months" in lower:
        return 12
    return default


# ── Build a flat label → (concept, months) lookup ─────────────────────────

def _build_label_lookup(ytd_months: int) -> dict:
    """
    Merge INCOME_STATEMENT_MAP and CASH_FLOW_MAP into a flat dict:
        label_substring_lower → (concept, months)

    Prefer entries whose months matches ytd_months; also keep instant (0).
    """
    lookup = {}
    combined = list(INCOME_STATEMENT_MAP.items()) + list(CASH_FLOW_MAP.items())
    for (concept, months), label_sub in combined:
        key = label_sub.lower()
        if key in lookup:
            continue  # first match wins
        if months in (ytd_months, 0):
            lookup[key] = (concept, months)
    return lookup


# ── Public API ─────────────────────────────────────────────────────────────

def process_html_tables(soup, target_config: dict, edgar_lookup: dict) -> dict:
    """
    Iterate over all <table> elements in *soup* (mutated in-place):

      • Identify tables that have the target or comparable period label in a header.
      • Blank numeric cells in the current-period column.
      • Fill comparable-period cells from edgar_lookup.

    Parameters
    ----------
    soup           : BeautifulSoup object (modified in-place)
    target_config  : dict with period_label, comparable_label, comparable_end,
                     and ytd_months keys
    edgar_lookup   : dict from build_fact_lookup(); may be empty

    Returns
    -------
    stats dict: tables_processed, cells_blanked, edgar_inserted, edgar_missing
    """
    tgt_label  = target_config["period_label"]          # "March 31, 2026"
    cmp_label  = target_config["comparable_label"]      # "March 31, 2025"
    cmp_end    = target_config["comparable_end"]        # "2025-03-31"
    ytd_months = int(target_config.get("ytd_months", 3))

    label_lookup = _build_label_lookup(ytd_months)

    stats = {
        "tables_processed": 0,
        "cells_blanked": 0,
        "edgar_inserted": 0,
        "edgar_missing": [],
    }

    for table in soup.find_all("table"):
        # ── Locate header row ──────────────────────────────────────────────
        thead = table.find("thead")
        header_tr = (thead.find("tr") if thead else None) or table.find("tr")
        if not header_tr:
            continue

        hdr_map = _header_col_map(header_tr)
        if not hdr_map:
            continue

        # ── Identify current-period and comparable-period columns ──────────
        current_col = comparable_col = None
        header_text_for_months = ""
        for col_idx, hdr_cell in hdr_map.items():
            cell_txt = _cell_text(hdr_cell)
            if tgt_label.lower() in cell_txt.lower():
                current_col = col_idx
                header_text_for_months = cell_txt
            if cmp_label.lower() in cell_txt.lower():
                comparable_col = col_idx

        if current_col is None:
            continue  # not a financial table for our roll-forward period

        # Re-infer months from column header text (may differ from ytd_months)
        col_months = _infer_months(header_text_for_months, ytd_months)
        col_label_lookup = _build_label_lookup(col_months)

        stats["tables_processed"] += 1
        logger.debug(
            "HTML table: current_col=%d comparable_col=%s col_months=%d",
            current_col, comparable_col, col_months,
        )

        # ── Iterate data rows ──────────────────────────────────────────────
        tbody = table.find("tbody") or table
        for row in tbody.find_all("tr", recursive=False):
            if row is header_tr:
                continue
            cells = _row_cells_by_col(row)
            if not cells:
                continue

            label_text = _cell_text(cells.get(0, "")).lower() if cells.get(0) else ""

            # Blank current-period column
            curr_cell = cells.get(current_col)
            if curr_cell is not None and _blank_cell(curr_cell):
                stats["cells_blanked"] += 1

            # Fill comparable column from EDGAR
            if edgar_lookup and comparable_col is not None:
                cmp_cell = cells.get(comparable_col)
                if cmp_cell is not None:
                    for label_sub, (concept, months) in col_label_lookup.items():
                        if label_sub in label_text:
                            val = edgar_lookup.get((concept, cmp_end, months))
                            hint = get_format_hint(concept)
                            if val is not None:
                                _set_cell_text(cmp_cell, _format_value(val, hint))
                                stats["edgar_inserted"] += 1
                            else:
                                stats["edgar_missing"].append(
                                    f"{concept}@{cmp_end}/{months}mo"
                                )
                            break

    logger.info(
        "HTML table pass: tables=%d blanked=%d inserted=%d missing=%d",
        stats["tables_processed"], stats["cells_blanked"],
        stats["edgar_inserted"], len(stats["edgar_missing"]),
    )
    return stats
