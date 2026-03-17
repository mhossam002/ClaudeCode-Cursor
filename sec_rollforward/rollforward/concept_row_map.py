"""
concept_row_map.py — Static mapping of (xbrl_concept, months) → row label substring
to match in Word tables for EDGAR value insertion.
"""

# Income statement row label substrings (case-insensitive match against col 0)
# Includes both standard us-gaap concept names and DJCO-specific alternatives.
# When multiple concepts map to the same row label, the first match wins
# (dict preserves insertion order in Python 3.7+).
INCOME_STATEMENT_MAP = {
    # 3-month concepts
    # DJCO uses RevenueFromContractWithCustomerIncludingAssessedTax, not Revenues
    ("RevenueFromContractWithCustomerIncludingAssessedTax", 3):         "revenues",
    ("Revenues", 3):                                                    "revenues",
    ("CostOfRevenue", 3):                                               "cost of",
    ("GrossProfit", 3):                                                 "gross profit",
    ("OperatingExpenses", 3):                                           "operating expenses",
    ("OperatingIncomeLoss", 3):                                         "operating income",
    ("NonoperatingIncomeExpense", 3):                                   "nonoperating",
    ("IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest", 3):
                                                                        "before income tax",
    ("IncomeTaxExpenseBenefit", 3):                                     "income tax",
    ("NetIncomeLoss", 3):                                               "net income",
    ("EarningsPerShareBasic", 3):                                       "basic",
    # DJCO does not file EarningsPerShareDiluted separately (equals basic)
    # ("EarningsPerShareDiluted", 3):                                   "diluted",
    ("WeightedAverageNumberOfSharesOutstandingBasic", 3):               "weighted average",
    # 6-month concepts (YTD)
    ("RevenueFromContractWithCustomerIncludingAssessedTax", 6):         "revenues",
    ("Revenues", 6):                                                    "revenues",
    ("CostOfRevenue", 6):                                               "cost of",
    ("GrossProfit", 6):                                                 "gross profit",
    ("OperatingExpenses", 6):                                           "operating expenses",
    ("OperatingIncomeLoss", 6):                                         "operating income",
    ("NonoperatingIncomeExpense", 6):                                   "nonoperating",
    ("IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest", 6):
                                                                        "before income tax",
    ("IncomeTaxExpenseBenefit", 6):                                     "income tax",
    ("NetIncomeLoss", 6):                                               "net income",
    ("EarningsPerShareBasic", 6):                                       "basic",
    # ("EarningsPerShareDiluted", 6):                                   "diluted",
    ("WeightedAverageNumberOfSharesOutstandingBasic", 6):               "weighted average",
}

# Cash flow row label substrings
CASH_FLOW_MAP = {
    ("NetCashProvidedByUsedInOperatingActivities", 6):                  "operating activities",
    ("NetCashProvidedByUsedInInvestingActivities", 6):                  "investing activities",
    ("NetCashProvidedByUsedInFinancingActivities", 6):                  "financing activities",
    ("CashAndCashEquivalentsAtCarryingValue", 0):                       "cash and cash equivalents",
}

# Format hints for EDGAR values
FORMAT_HINTS = {
    "EarningsPerShareBasic":    "per_share",
    "EarningsPerShareDiluted":  "per_share",
    "WeightedAverageNumberOfSharesOutstandingBasic": "shares",
    "WeightedAverageNumberOfSharesOutstandingDiluted": "shares",
}

def get_format_hint(concept: str) -> str:
    """Return format hint for a concept: 'currency', 'per_share', or 'shares'."""
    return FORMAT_HINTS.get(concept, "currency")
