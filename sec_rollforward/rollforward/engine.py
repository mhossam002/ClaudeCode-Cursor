"""
engine.py — Orchestrator: wires all modules, returns stats dict.
"""
import logging
import os

from .docx_parser import load_document, extract_table_map, classify_table_columns, detect_financial_tables
from .edgar_client import fetch_company_facts, build_fact_lookup, get_fact
from .text_updater import update_all_paragraphs, highlight_financial_paragraphs_for_review, build_rules
from .table_updater import (
    process_balance_sheet,
    process_income_statement,
    process_cash_flow,
    process_generic_disclosure_table,
    add_ytd_columns,
)
from .concept_row_map import INCOME_STATEMENT_MAP, CASH_FLOW_MAP

logger = logging.getLogger(__name__)

DJCO_CIK = "0000783412"

Q1_CONFIG = {
    "period_end":          "2025-12-31",
    "period_label":        "December 31, 2025",
    "quarter_name":        "first",
    "filing_date":         "February 17, 2026",
    "ytd_months":          3,
    "comparable_end":      "2024-12-31",
    "comparable_label":    "December 31, 2024",
    "prior_year_end_label":"September 30, 2025",
}

Q2_CONFIG = {
    "period_end":          "2026-03-31",
    "period_label":        "March 31, 2026",
    "quarter_name":        "second",
    "filing_date":         "[FILING DATE]",
    "ytd_months":          6,
    "comparable_end":      "2025-03-31",
    "comparable_label":    "March 31, 2025",
    "prior_year_end_label":"September 30, 2025",   # unchanged
}

# ---------------------------------------------------------------------------
# Table index assignments (0-based — verified by diagnose.py against source)
# ---------------------------------------------------------------------------
# Table  3: Table of Contents (26r×4c) — skip
# Table  4: Balance Sheet (35r×5c) — "December 31, 2025" vs "September 30, 2025"
# Table  5: Income Statement (35r×5c) — "Three Months Ended December 31,"
# Table  6: Stockholders' Equity (13r×14c) — skip (period-neutral rollforward)
# Table  7: Cash Flow Statement (35r×5c) — "December 31, 2025" vs "December 31, 2024"
# Table  8: A/R rollforward (8r×4c) — generic date update
# Table  9: Allowance for credit losses (5r×9c) — generic date update
# Table 10: Revenue by geography (8r×14c) — generic date update
# Table 11: Deferred revenue snapshot (5r×6c) — generic date update
# Table 12: Deferred revenue rollforward (8r×7c) — generic date update
# Table 13: Fair value current period (4r×13c) — generic date update
# Table 15: Securities detail (4r×12c) — generic date update
# Table 17: Accrued liabilities (6r×6c) — generic date update
# Table 18: EPS table (11r×7c) — income statement-like columns
# Table 20: Segment assets (3r×32c) — generic date update
# Table 22: Cash flow MD&A summary (6r×9c) — cash-flow-like columns
TABLE_BALANCE_SHEET    = 4
TABLE_INCOME_STMT      = 5
TABLE_CASH_FLOW        = 7
TABLE_INCOME_STMT_ALT  = 18   # EPS table in notes (same period columns)
TABLE_CASH_FLOW_ALT    = 22   # Cash flow MD&A summary table
TABLES_GENERIC         = [8, 9, 10, 11, 12, 13, 15, 17, 20]


def verify_output(
    doc,
    source_config: dict,
    target_config: dict,
    edgar_lookup: dict,
    table_indices: dict = None,
) -> dict:
    """
    Automated verification checks.
    Returns dict with pass/fail flags and details.

    Parameters
    ----------
    table_indices : optional dict from detect_financial_tables().  When
                    provided the detected indices are used; otherwise the
                    module-level hardcoded constants serve as fallbacks.
    """
    _tbl_balance_sheet = (
        table_indices.get("balance_sheet", TABLE_BALANCE_SHEET)
        if table_indices else TABLE_BALANCE_SHEET
    )
    _tbl_income_stmt = (
        table_indices.get("income_statement", TABLE_INCOME_STMT)
        if table_indices else TABLE_INCOME_STMT
    )

    results = {
        "no_source_dates_remain":        True,
        "balance_sheet_col2_intact":     False,
        "income_stmt_comparable_filled": False,
        "issues":                        [],
    }

    src_date = source_config["period_label"]

    # Check 1: no source dates remain
    for para in doc.paragraphs:
        if src_date in para.text:
            results["no_source_dates_remain"] = False
            results["issues"].append(f"Source date found in paragraph: '{para.text[:80]}'")
    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                if src_date in cell.text:
                    results["no_source_dates_remain"] = False
                    results["issues"].append(f"Source date found in table cell: '{cell.text[:60]}'")

    # Check 2: Balance sheet header row still has Sep 30, 2025 somewhere
    if _tbl_balance_sheet < len(doc.tables):
        bs_table = doc.tables[_tbl_balance_sheet]
        if bs_table.rows:
            num_cols = len(bs_table.columns)
            found_sep30 = False
            for c in range(num_cols):
                try:
                    cell_text = bs_table.cell(0, c).text
                    if "September 30" in cell_text or "Sep" in cell_text:
                        found_sep30 = True
                        break
                except Exception:
                    pass
            results["balance_sheet_col2_intact"] = found_sep30
            if not found_sep30:
                # Show what's actually in the header row
                header_texts = []
                for c in range(num_cols):
                    try:
                        header_texts.append(bs_table.cell(0, c).text.strip()[:30])
                    except Exception:
                        pass
                results["issues"].append(
                    f"Balance sheet Sep 30 header not found. Row 0 cells: {header_texts}"
                )

    # Check 3: Income statement comparable column has ≥3 non-blank numeric cells
    if _tbl_income_stmt < len(doc.tables):
        is_table = doc.tables[_tbl_income_stmt]
        from .docx_parser import get_unique_cells_in_row, is_numeric_cell
        filled_count = 0
        # Comparable is typically col 2 — scan all cols beyond label
        for row in is_table.rows[1:]:
            ucells = get_unique_cells_in_row(row)
            for c_idx in range(1, len(ucells)):
                txt = ucells[c_idx].text.strip()
                if is_numeric_cell(txt) and txt not in ("", "—", "–"):
                    filled_count += 1
                    break  # one per row is enough
        results["income_stmt_comparable_filled"] = filled_count >= 3
        if filled_count < 3:
            results["issues"].append(
                f"Income statement comparable column only has {filled_count} filled cells (need ≥3)"
            )

    return results


def roll_forward(
    source_path: str,
    output_path: str,
    cik: str = DJCO_CIK,
    user_agent: str = "DJCO SEC Tool admin@example.com",
    add_ytd: bool = False,
    source_config: dict = None,
    target_config: dict = None,
    progress_callback=None,
) -> dict:
    """
    Main orchestrator.

    Parameters
    ----------
    source_path       : path to source .docx (never modified)
    output_path       : path to write the new draft .docx
    cik               : SEC CIK used for EDGAR fact lookup
    user_agent        : SEC EDGAR API User-Agent header (required by SEC)
    add_ytd           : if True, run the lxml YTD column expansion (default False)
    source_config     : period config for the source document (defaults to Q1_CONFIG)
    target_config     : period config for the target document  (defaults to Q2_CONFIG)
    progress_callback : optional callable(message: str) invoked at each milestone

    Returns
    -------
    stats dict: status, replacements_made, tables_processed,
                edgar_facts_found, edgar_facts_missing, warnings, verification,
                table_detection, changes
    """
    src_cfg = source_config or Q1_CONFIG
    tgt_cfg = target_config or Q2_CONFIG

    stats = {
        "status": "error",
        "replacements_made": 0,
        "tables_processed": 0,
        "edgar_facts_found": 0,
        "edgar_facts_missing": [],
        "warnings": [],
        "verification": {},
        "table_detection": {},
        "changes": [],
    }

    # Helper: log and optionally forward progress to caller
    def emit(msg: str):
        logger.info(msg)
        if progress_callback:
            progress_callback(msg)

    # --- 1. Load source document (never modify original) ---
    emit("Loading document...")
    doc = load_document(source_path)

    # --- 1b. Content-based table detection (feature 1.1) ---
    detected = detect_financial_tables(doc)
    stats["table_detection"] = detected
    # Build a display-friendly list of detected type names (exclude "detected_at" meta key)
    detected_types = [k for k in detected if k != "detected_at"]
    emit(f"Table detection: found {', '.join(detected_types)} tables")

    # Resolve table indices — detected values take priority; DJCO constants are fallbacks
    tbl_balance_sheet   = detected.get("balance_sheet",    TABLE_BALANCE_SHEET)
    tbl_income_stmt     = detected.get("income_statement", TABLE_INCOME_STMT)
    tbl_cash_flow       = detected.get("cash_flow",        TABLE_CASH_FLOW)
    tbl_income_stmt_alt = detected.get("eps",              TABLE_INCOME_STMT_ALT)
    tbl_cash_flow_alt   = detected.get("cash_flow_alt",    TABLE_CASH_FLOW_ALT)

    # Any detected type not already in the primary set goes into the generic pool
    primary_indices = {
        tbl_balance_sheet, tbl_income_stmt, tbl_cash_flow,
        tbl_income_stmt_alt, tbl_cash_flow_alt,
    }
    tables_generic = list(TABLES_GENERIC)
    for key, idx in detected.items():
        if key == "detected_at":
            continue
        if idx not in primary_indices and idx not in tables_generic:
            tables_generic.append(idx)

    # --- 2. Fetch EDGAR data ---
    emit("Fetching EDGAR data...")
    try:
        facts_json = fetch_company_facts(cik, user_agent)
        edgar_lookup = build_fact_lookup(facts_json)
        emit(f"EDGAR: {len(edgar_lookup)} facts loaded")
    except Exception as exc:
        warning = f"EDGAR fetch failed: {exc}. Comparable cells will be left blank."
        logger.warning(warning)
        stats["warnings"].append(warning)
        edgar_lookup = {}
        emit("EDGAR fetch failed — comparable cells will be left blank")

    comparable_end = tgt_cfg["comparable_end"]

    # --- 3. Text replacement pass (rules derived from configs) ---
    emit("Text replacement pass...")
    rules = build_rules(src_cfg, tgt_cfg)
    stats["replacements_made"] = update_all_paragraphs(doc, rules)
    emit(f"Text replacement: {stats['replacements_made']} substitutions")

    # Change tracking — text replacement
    stats["changes"].append({
        "type": "text_replacement",
        "count": stats["replacements_made"],
        "detail": f"{stats['replacements_made']} date/period substitutions across paragraphs and tables",
    })

    # --- 4. Highlight MD&A financial paragraphs for manual review ---
    highlight_financial_paragraphs_for_review(doc)

    # --- 5. Table processing ---
    table_count = len(doc.tables)
    logger.info("Document has %d tables", table_count)

    def safe_get_table(idx):
        if idx < table_count:
            return doc.tables[idx]
        stats["warnings"].append(f"Table index {idx} out of range (doc has {table_count} tables)")
        return None

    # Balance Sheet
    emit("Processing Balance Sheet...")
    bs_table = safe_get_table(tbl_balance_sheet)
    if bs_table is not None:
        process_balance_sheet(bs_table, tgt_cfg)
        stats["tables_processed"] += 1
        stats["changes"].append({
            "type": "table_blanked",
            "table": "Balance Sheet",
            "detail": "Current-period column blanked",
        })

    # Income Statement (primary + notes repeat)
    emit("Processing Income Statement...")
    for tbl_idx in (tbl_income_stmt, tbl_income_stmt_alt):
        tbl = safe_get_table(tbl_idx)
        if tbl is not None:
            col_map = classify_table_columns(tbl, src_cfg, tgt_cfg)
            is_stats = process_income_statement(
                tbl, col_map, edgar_lookup, INCOME_STATEMENT_MAP,
                comparable_end, months=3
            )
            stats["edgar_facts_found"] += is_stats["edgar_inserted"]
            stats["edgar_facts_missing"].extend(is_stats["edgar_missing"])
            stats["tables_processed"] += 1
            tbl_label = "Income Statement" if tbl_idx == tbl_income_stmt else "EPS / Income Statement (alt)"
            stats["changes"].append({
                "type": "table_blanked",
                "table": tbl_label,
                "detail": "Current-period column blanked",
            })

            if add_ytd and tbl_idx == tbl_income_stmt:
                ytd_stats = add_ytd_columns(tbl, edgar_lookup, tgt_cfg, comparable_end)
                stats["edgar_facts_found"] += ytd_stats["edgar_inserted"]
                stats["edgar_facts_missing"].extend(ytd_stats["edgar_missing"])

    # Cash Flow (primary + notes repeat)
    emit("Processing Cash Flow...")
    for tbl_idx in (tbl_cash_flow, tbl_cash_flow_alt):
        tbl = safe_get_table(tbl_idx)
        if tbl is not None:
            col_map = classify_table_columns(tbl, src_cfg, tgt_cfg)
            cf_stats = process_cash_flow(tbl, col_map, edgar_lookup, comparable_end)
            stats["edgar_facts_found"] += cf_stats["edgar_inserted"]
            stats["edgar_facts_missing"].extend(cf_stats["edgar_missing"])
            stats["tables_processed"] += 1
            tbl_label = "Cash Flow" if tbl_idx == tbl_cash_flow else "Cash Flow (alt)"
            stats["changes"].append({
                "type": "table_blanked",
                "table": tbl_label,
                "detail": "Current-period column blanked",
            })

    # Generic disclosure tables
    emit(f"Processing {len(tables_generic)} disclosure tables...")
    for tbl_idx in tables_generic:
        tbl = safe_get_table(tbl_idx)
        if tbl is not None:
            col_map = classify_table_columns(tbl, src_cfg, tgt_cfg)
            process_generic_disclosure_table(tbl, col_map, tgt_cfg)
            stats["tables_processed"] += 1

    # Change tracking — EDGAR insertions (after all table processing is done)
    stats["changes"].append({
        "type": "edgar_inserted",
        "count": stats["edgar_facts_found"],
        "detail": (
            f"{stats['edgar_facts_found']} EDGAR facts inserted into comparable-period columns"
        ),
    })

    # --- 6. Verify output ---
    emit("Running verification...")
    stats["verification"] = verify_output(
        doc, src_cfg, tgt_cfg, edgar_lookup, table_indices=detected
    )

    # --- 7. Save output ---
    emit("Saving output...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    doc.save(output_path)

    stats["status"] = "ok"
    logger.info("Roll-forward complete. Stats: %s", {
        k: v for k, v in stats.items() if k not in ("verification", "table_detection", "changes")
    })
    return stats
