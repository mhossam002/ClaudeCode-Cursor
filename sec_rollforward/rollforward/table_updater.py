"""
table_updater.py — Table column blanking, EDGAR value insertion, YTD columns.
"""
import logging
import copy
from lxml import etree

from .docx_parser import is_numeric_cell, get_unique_cells_in_row
from .concept_row_map import (
    INCOME_STATEMENT_MAP, CASH_FLOW_MAP, get_format_hint
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cell-level helpers
# ---------------------------------------------------------------------------

def blank_cell(cell, placeholder: str = "") -> bool:
    """
    Replace numeric cell content with placeholder (default empty).
    Preserves run formatting. Returns True if blanked.
    """
    text = cell.text.strip()
    if not is_numeric_cell(text):
        return False
    for para in cell.paragraphs:
        for i, run in enumerate(para.runs):
            run.text = placeholder if i == 0 else ""
        # If no runs, clear paragraph directly
        if not para.runs:
            para.clear()
    return True


def _format_value(value, format_hint: str) -> str:
    """Format a numeric value for insertion into a Word cell."""
    if value is None:
        return ""
    try:
        if format_hint == "per_share":
            return f"{float(value):.2f}"
        elif format_hint == "shares":
            return f"{int(round(float(value))):,}"
        else:  # currency — typically in thousands
            return f"{int(round(float(value))):,}"
    except (ValueError, TypeError):
        return str(value)


def insert_edgar_value(cell, value, format_hint: str = "currency") -> bool:
    """
    Write an EDGAR value into a cell.
    Returns True if a value was written, False if value is None.
    """
    if value is None:
        return False
    formatted = _format_value(value, format_hint)
    # Write into first run of first paragraph, clear others
    para = cell.paragraphs[0] if cell.paragraphs else None
    if para is None:
        return False
    if para.runs:
        para.runs[0].text = formatted
        for run in para.runs[1:]:
            run.text = ""
    else:
        para.add_run(formatted)
    return True


def find_row_by_label(table, label_substring: str):
    """
    Case-insensitive substring scan of column 0.
    Returns (row_index, row) or (None, None) if not found.
    """
    label_lower = label_substring.lower()
    for r_idx, row in enumerate(table.rows):
        cells = get_unique_cells_in_row(row)
        if cells and label_lower in cells[0].text.lower():
            return r_idx, row
    return None, None


# ---------------------------------------------------------------------------
# Balance Sheet
# ---------------------------------------------------------------------------

def process_balance_sheet(table, target_config: dict) -> dict:
    """
    Balance Sheet table processor.
    - Identifies the current-period column by scanning header row for the
      target period label (already updated by text_updater pass).
    - Blanks all numeric cells in that column.
    - Leaves the September 30 prior-year-end column completely untouched.
    Returns stats dict.
    """
    stats = {"blanked": 0, "current_col": None}
    tgt_label = target_config["period_label"]          # "March 31, 2026"
    prior_ye   = target_config["prior_year_end_label"] # "September 30, 2025"

    num_cols = len(table.columns)

    # Scan header row (row 0) across all columns to find current-period col
    current_col = None
    prior_ye_col = None
    for c in range(num_cols):
        try:
            cell_text = table.cell(0, c).text.strip()
        except Exception:
            continue
        if tgt_label.lower() in cell_text.lower():
            current_col = c
        if "september 30" in cell_text.lower() or prior_ye.lower() in cell_text.lower():
            prior_ye_col = c

    if current_col is None:
        # Fallback: try col 2 (typical position in 5-col balance sheet)
        logger.warning("Balance sheet: could not find current-period header, falling back to col 2")
        current_col = 2

    stats["current_col"] = current_col
    logger.info("Balance sheet: current_col=%d prior_ye_col=%s", current_col, prior_ye_col)

    for r_idx, row in enumerate(table.rows):
        if r_idx == 0:
            continue  # header already updated by text_updater pass
        try:
            cell = table.cell(r_idx, current_col)
        except Exception:
            continue
        if blank_cell(cell):
            stats["blanked"] += 1

    logger.info("Balance sheet: blanked %d cells in col %d", stats["blanked"], current_col)
    return stats


# ---------------------------------------------------------------------------
# Income Statement
# ---------------------------------------------------------------------------

def process_income_statement(table, col_map: list, edgar_lookup: dict,
                              concept_row_map: dict, comparable_period_end: str,
                              months: int = 3) -> dict:
    """
    - Blank current-period column (CURRENT_PERIOD role)
    - Populate comparable column from EDGAR (COMPARABLE role)
    Returns stats dict.
    """
    stats = {"blanked": 0, "edgar_inserted": 0, "edgar_missing": []}

    current_cols = [c["col_index"] for c in col_map if c["role"] == "CURRENT_PERIOD"]
    comparable_cols = [c["col_index"] for c in col_map if c["role"] == "COMPARABLE"]

    for r_idx, row in enumerate(table.rows):
        cells = get_unique_cells_in_row(row)
        label_text = cells[0].text.strip().lower() if cells else ""

        # Blank current-period cells
        for col_idx in current_cols:
            if col_idx < len(cells):
                if blank_cell(cells[col_idx]):
                    stats["blanked"] += 1

        # Populate comparable cells from EDGAR
        for col_idx in comparable_cols:
            if col_idx < len(cells):
                # Find matching concept by row label
                for (concept, m), label_sub in concept_row_map.items():
                    if m != months:
                        continue
                    if label_sub.lower() in label_text:
                        val = edgar_lookup.get((concept, comparable_period_end, months))
                        hint = get_format_hint(concept)
                        if insert_edgar_value(cells[col_idx], val, hint):
                            stats["edgar_inserted"] += 1
                        else:
                            stats["edgar_missing"].append(f"{concept}@{comparable_period_end}/{months}mo")
                        break

    logger.info("Income statement: blanked=%d inserted=%d missing=%d",
                stats["blanked"], stats["edgar_inserted"], len(stats["edgar_missing"]))
    return stats


# ---------------------------------------------------------------------------
# Cash Flow
# ---------------------------------------------------------------------------

def process_cash_flow(table, col_map: list, edgar_lookup: dict,
                      comparable_period_end: str) -> dict:
    """
    - Blank current YTD column (CURRENT_PERIOD or YTD_CURRENT role)
    - Populate comparable YTD (6-mo) from EDGAR
    Returns stats dict.
    """
    stats = {"blanked": 0, "edgar_inserted": 0, "edgar_missing": []}

    current_cols = [c["col_index"] for c in col_map
                    if c["role"] in ("CURRENT_PERIOD", "YTD_CURRENT")]
    comparable_cols = [c["col_index"] for c in col_map
                       if c["role"] in ("COMPARABLE", "YTD_COMPARABLE")]

    for r_idx, row in enumerate(table.rows):
        cells = get_unique_cells_in_row(row)
        label_text = cells[0].text.strip().lower() if cells else ""

        for col_idx in current_cols:
            if col_idx < len(cells):
                if blank_cell(cells[col_idx]):
                    stats["blanked"] += 1

        for col_idx in comparable_cols:
            if col_idx < len(cells):
                for (concept, months), label_sub in CASH_FLOW_MAP.items():
                    if label_sub.lower() in label_text:
                        val = edgar_lookup.get((concept, comparable_period_end, months))
                        hint = get_format_hint(concept)
                        if insert_edgar_value(cells[col_idx], val, hint):
                            stats["edgar_inserted"] += 1
                        else:
                            stats["edgar_missing"].append(f"{concept}@{comparable_period_end}/{months}mo")
                        break

    logger.info("Cash flow: blanked=%d inserted=%d missing=%d",
                stats["blanked"], stats["edgar_inserted"], len(stats["edgar_missing"]))
    return stats


# ---------------------------------------------------------------------------
# Generic Disclosure Tables
# ---------------------------------------------------------------------------

def process_generic_disclosure_table(table, col_map: list, target_config: dict) -> dict:
    """
    For disclosure tables (notes, schedules, etc.):
    - Update date header cells in CURRENT_PERIOD columns
    - Blank current-period numeric cells
    - Leave prior-period columns untouched
    Returns stats dict.
    """
    stats = {"blanked": 0, "header_updated": 0}
    tgt_label = target_config["period_label"]
    current_cols = [c["col_index"] for c in col_map if c["role"] == "CURRENT_PERIOD"]

    for r_idx, row in enumerate(table.rows):
        cells = get_unique_cells_in_row(row)

        for col_idx in current_cols:
            if col_idx >= len(cells):
                continue
            cell = cells[col_idx]
            cell_text = cell.text.strip()

            if r_idx == 0:
                # Header row — update date label
                for para in cell.paragraphs:
                    for run in para.runs:
                        # Replace source period references
                        for old_date in ("December 31, 2025", "December 31, 2024"):
                            if old_date in run.text:
                                run.text = run.text.replace(old_date, tgt_label)
                                stats["header_updated"] += 1
            else:
                if blank_cell(cell):
                    stats["blanked"] += 1

    logger.info("Generic table: blanked=%d header_updated=%d", stats["blanked"], stats["header_updated"])
    return stats


# ---------------------------------------------------------------------------
# YTD Column Addition (Q1 → Q2 structural change)
# ---------------------------------------------------------------------------

_NSMAP = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
_W = "{%s}" % _NSMAP


def _make_grid_col(width_twips: int = 1440) -> etree.Element:
    col = etree.Element(_W + "gridCol")
    col.set(_W + "w", str(width_twips))
    return col


def add_ytd_columns(table, edgar_lookup: dict, target_config: dict,
                    comparable_period_end: str) -> dict:
    """
    Q1→Q2 structural change: expand income statement from 3 columns to 5.
    Uses lxml XML manipulation on table._tbl.

    Layout after expansion:
      Col 0: Label
      Col 1: 3-mo current (March 31, 2026) — blank
      Col 2: 3-mo comparable (March 31, 2025) — from EDGAR
      Col 3: 6-mo YTD current — blank
      Col 4: 6-mo YTD comparable — from EDGAR

    Returns stats dict.
    """
    stats = {"cols_added": 0, "edgar_inserted": 0, "edgar_missing": []}
    tbl = table._tbl

    # 1. Extend tblGrid
    tbl_grid = tbl.find(_W + "tblGrid")
    if tbl_grid is None:
        logger.warning("add_ytd_columns: no tblGrid found, skipping")
        return stats

    existing_cols = tbl_grid.findall(_W + "gridCol")
    if existing_cols:
        avg_width = int(existing_cols[-1].get(_W + "w", "1440"))
    else:
        avg_width = 1440

    tbl_grid.append(_make_grid_col(avg_width))
    tbl_grid.append(_make_grid_col(avg_width))
    stats["cols_added"] = 2

    # 2. For each row: clone col-1 → new col-3, clone col-2 → new col-4
    tgt_ytd_label = f"Six Months Ended\n{target_config['period_label']}"
    cmp_ytd_label = f"Six Months Ended\n{target_config['comparable_label']}"

    for r_idx, row in enumerate(table.rows):
        tr = row._tr
        cells = get_unique_cells_in_row(row)
        if len(cells) < 3:
            continue

        # Clone cells
        col1_tc = cells[1]._tc
        col2_tc = cells[2]._tc
        new_ytd_current = copy.deepcopy(col1_tc)
        new_ytd_comparable = copy.deepcopy(col2_tc)

        # Clear numeric content in new cells
        for tc in (new_ytd_current, new_ytd_comparable):
            for t_elem in tc.iter(_W + "t"):
                t_elem.text = ""

        # Append new cells to row
        tr.append(new_ytd_current)
        tr.append(new_ytd_comparable)

        if r_idx == 0:
            # Update header labels
            _set_tc_text(new_ytd_current, tgt_ytd_label)
            _set_tc_text(new_ytd_comparable, cmp_ytd_label)
        else:
            # Populate comparable YTD from EDGAR
            label_text = cells[0].text.strip().lower()
            for (concept, months), label_sub in INCOME_STATEMENT_MAP.items():
                if months != 6:
                    continue
                if label_sub.lower() in label_text:
                    val = edgar_lookup.get((concept, comparable_period_end, 6))
                    hint = get_format_hint(concept)
                    if val is not None:
                        _set_tc_text(new_ytd_comparable, _format_value(val, hint))
                        stats["edgar_inserted"] += 1
                    else:
                        stats["edgar_missing"].append(f"{concept}@{comparable_period_end}/6mo")
                    break

    logger.info("add_ytd_columns: cols_added=%d edgar_inserted=%d missing=%d",
                stats["cols_added"], stats["edgar_inserted"], len(stats["edgar_missing"]))
    return stats


def _format_value(value, format_hint: str) -> str:
    """Format a numeric value for insertion (same as in cell helpers)."""
    if value is None:
        return ""
    try:
        if format_hint == "per_share":
            return f"{float(value):.2f}"
        elif format_hint == "shares":
            return f"{int(round(float(value))):,}"
        else:
            return f"{int(round(float(value))):,}"
    except (ValueError, TypeError):
        return str(value)


def _set_tc_text(tc_elem, text: str):
    """Set the text content of a table cell XML element."""
    for t_elem in tc_elem.iter(_W + "t"):
        t_elem.text = ""
    # Find or create first paragraph/run
    p_elems = tc_elem.findall(".//" + _W + "p")
    if p_elems:
        p = p_elems[0]
        r_elems = p.findall(_W + "r")
        if r_elems:
            t_list = r_elems[0].findall(_W + "t")
            if t_list:
                t_list[0].text = text
                return
            t_new = etree.SubElement(r_elems[0], _W + "t")
            t_new.text = text
        else:
            r_new = etree.SubElement(p, _W + "r")
            t_new = etree.SubElement(r_new, _W + "t")
            t_new.text = text
