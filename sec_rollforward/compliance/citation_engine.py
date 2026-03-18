"""
citation_engine.py — Maps document sections to regulatory requirements.
Provides regulatory rationale for each financial statement section.
"""

import logging
from typing import Optional

from . import knowledge_base as kb

logger = logging.getLogger(__name__)

_MODEL = "claude-haiku-4-5-20251001"
_MAX_TOKENS = 800

# ---------------------------------------------------------------------------
# Section → regulatory topic mapping
# ---------------------------------------------------------------------------

SECTION_TOPICS: dict = {
    "balance_sheet": {
        "query": "balance sheet presentation requirements interim quarterly",
        "standards": ["Reg S-X Article 10 §10-01", "ASC 210", "ASC 470"],
    },
    "income_statement": {
        "query": "income statement presentation interim quarterly reporting",
        "standards": ["Reg S-X §10-02", "ASC 220", "ASC 270"],
    },
    "cash_flow": {
        "query": "cash flow statement requirements interim reporting",
        "standards": ["ASC 230", "Reg S-X §10-04"],
    },
    "mda": {
        "query": "MD&A management discussion analysis requirements quarterly",
        "standards": ["Reg S-K Item 303"],
    },
    "eps": {
        "query": "earnings per share disclosure requirements",
        "standards": ["ASC 260", "Reg S-X §10-02"],
    },
    "notes": {
        "query": "footnote disclosure requirements interim financial statements",
        "standards": ["ASC 270", "Reg S-X Article 10"],
    },
}


# ---------------------------------------------------------------------------
# Citation retrieval
# ---------------------------------------------------------------------------


def get_citations_for_section(section_type: str, collection) -> list:
    """Retrieve top regulatory citations for a given financial statement section.

    Performs a semantic search against the knowledge base. Falls back to the
    hard-coded ``SECTION_TOPICS`` standards list if the collection is empty
    or unavailable.

    Args:
        section_type: One of the keys in :data:`SECTION_TOPICS`
                      (e.g. ``"balance_sheet"``).
        collection:   ChromaDB collection from
                      :func:`knowledge_base.get_collection`.

    Returns:
        List of ``{source_name, text_excerpt, relevance_score}`` dicts.
        Maximum 5 results.
    """
    topic = SECTION_TOPICS.get(section_type)
    if not topic:
        logger.warning("citation_engine: unknown section_type '%s'", section_type)
        return []

    query = topic["query"]
    fallback_standards = topic["standards"]

    try:
        chunks = kb.search(collection, query, n_results=5)
    except Exception as exc:
        logger.warning(
            "citation_engine: search failed for '%s': %s — using fallback", section_type, exc
        )
        chunks = []

    if chunks:
        return [
            {
                "source_name": c.get("source_name", c.get("source_id", "Unknown")),
                "text_excerpt": c.get("text", "")[:400],
                "relevance_score": round(1.0 - c.get("distance", 1.0), 4),
            }
            for c in chunks
        ]

    # Fallback: synthesise placeholder citations from the standards list
    logger.info(
        "citation_engine: no KB results for '%s', returning fallback standards", section_type
    )
    return [
        {
            "source_name": std,
            "text_excerpt": f"See {std} for {section_type.replace('_', ' ')} requirements.",
            "relevance_score": 0.0,
        }
        for std in fallback_standards
    ]


# ---------------------------------------------------------------------------
# Disclosure explanation
# ---------------------------------------------------------------------------


def explain_disclosure(
    section_type: str,
    context: str,
    collection,
    api_key: str,
) -> dict:
    """Generate a plain-English explanation of why a section is required and
    what it must contain, grounded in regulatory citations.

    Args:
        section_type: One of the keys in :data:`SECTION_TOPICS`.
        context:      Additional context (e.g. company name, reporting period).
        collection:   ChromaDB collection.
        api_key:      Anthropic API key.

    Returns:
        Dict with keys:
        ``rationale`` (str), ``citations`` (list of citation dicts),
        ``standards`` (list of standard name strings).
    """
    try:
        import anthropic
    except ImportError:
        raise RuntimeError(
            "anthropic package not installed. Run: pip install anthropic"
        )

    topic = SECTION_TOPICS.get(section_type, {})
    standards = topic.get("standards", [])
    section_label = section_type.replace("_", " ").title()

    citations = get_citations_for_section(section_type, collection)
    citation_block = ""
    for i, cit in enumerate(citations, 1):
        citation_block += f"[{i}] {cit['source_name']}:\n{cit['text_excerpt']}\n\n"

    user_message = (
        f"Explain the regulatory requirements for the '{section_label}' section "
        f"of a quarterly SEC Form 10-Q filing.\n\n"
        f"Context: {context}\n\n"
        f"Relevant regulatory excerpts:\n{citation_block}"
        f"Applicable standards: {', '.join(standards)}\n\n"
        "Provide a concise plain-English explanation covering: (1) why this section "
        "is required, (2) what it must contain, and (3) key compliance considerations. "
        "Cite your sources using [Source Name] notation."
    )

    client = anthropic.Anthropic(api_key=api_key)
    try:
        response = client.messages.create(
            model=_MODEL,
            max_tokens=_MAX_TOKENS,
            system=(
                "You are an expert SEC compliance advisor. Provide concise, accurate "
                "regulatory guidance for financial reporting professionals."
            ),
            messages=[{"role": "user", "content": user_message}],
        )
        rationale = response.content[0].text if response.content else ""
    except Exception as exc:
        logger.error(
            "citation_engine.explain_disclosure: API error for '%s': %s", section_type, exc
        )
        rationale = f"Error retrieving explanation: {exc}"

    return {
        "rationale": rationale,
        "citations": citations,
        "standards": standards,
    }
