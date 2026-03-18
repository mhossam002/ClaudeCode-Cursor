"""
ai_assistant.py — Claude API integration for MD&A narrative suggestions.
Analyzes yellow-highlighted paragraphs and suggests updated versions.
"""

import logging
from typing import Generator

logger = logging.getLogger(__name__)

_MODEL = "claude-haiku-4-5-20251001"
_MAX_TOKENS = 500
_BATCH_SIZE = 5  # paragraphs per Claude API call


# ---------------------------------------------------------------------------
# Document scanning
# ---------------------------------------------------------------------------


def extract_highlighted_paragraphs(doc) -> list:
    """Scan a python-docx Document for yellow-highlighted paragraphs that contain '$'.

    A paragraph is included only when at least one of its runs has
    ``highlight_color`` equal to ``"yellow"`` **and** the full paragraph text
    contains the dollar-sign character.

    Args:
        doc: A ``docx.Document`` instance.

    Returns:
        List of dicts with keys: ``index`` (int), ``text`` (str),
        ``has_dollar`` (bool — always True per the filter).
    """
    results = []
    for idx, para in enumerate(doc.paragraphs):
        has_highlight = any(
            getattr(run.font.highlight_color, "name", None) == "YELLOW"
            for run in para.runs
            if run.font.highlight_color is not None
        )
        if not has_highlight:
            continue
        text = para.text.strip()
        if "$" not in text:
            continue
        results.append({"index": idx, "text": text, "has_dollar": True})
    return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _quarter_label(config: dict) -> str:
    """Build a human-readable quarter label from a config dict."""
    return config.get("quarter_label", f"Q{config.get('quarter', '?')} {config.get('year', '')}")


def _build_user_prompt(
    paragraphs: list,
    source_config: dict,
    target_config: dict,
    edgar_facts: dict,
) -> str:
    """Build the user message for a batch of paragraphs."""
    src_label = _quarter_label(source_config)
    tgt_label = _quarter_label(target_config)

    facts_summary = ""
    if edgar_facts:
        lines = []
        for key, val in list(edgar_facts.items())[:20]:  # cap to 20 facts
            lines.append(f"  {key}: {val}")
        facts_summary = "\n".join(lines)
    else:
        facts_summary = "(no EDGAR facts available)"

    para_block = ""
    for i, p in enumerate(paragraphs, 1):
        para_block += f"\n[PARAGRAPH {i}] (doc_index={p['index']})\n{p['text']}\n"

    return (
        f"Update the following MD&A paragraph(s) from {src_label} to {tgt_label}.\n\n"
        f"Available EDGAR facts for the comparable period:\n{facts_summary}\n\n"
        f"For EACH paragraph below, provide ONLY the updated paragraph text. "
        f"Label your responses as [PARAGRAPH 1], [PARAGRAPH 2], etc.\n"
        f"{para_block}"
    )


def _parse_batch_response(response_text: str, paragraphs: list) -> list:
    """Split a batch response into per-paragraph suggestion strings."""
    results = [""] * len(paragraphs)
    import re

    # Try to split on [PARAGRAPH N] markers
    parts = re.split(r"\[PARAGRAPH\s+(\d+)\]", response_text)
    # parts will be: ["preamble", "1", "text1", "2", "text2", ...]
    if len(parts) >= 3:
        for i in range(1, len(parts) - 1, 2):
            try:
                idx = int(parts[i]) - 1  # 0-based
                if 0 <= idx < len(results):
                    results[idx] = parts[i + 1].strip()
            except (ValueError, IndexError):
                pass
    else:
        # Fallback: assign entire text to first paragraph
        if results:
            results[0] = response_text.strip()
    return results


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_mda_suggestions(
    paragraphs: list,
    source_config: dict,
    target_config: dict,
    edgar_facts: dict,
    api_key: str,
) -> list:
    """Generate updated MD&A paragraph suggestions using Claude API.

    Paragraphs are batched in groups of up to :data:`_BATCH_SIZE` to minimise
    API round-trips.

    Args:
        paragraphs:    List of ``{index: int, text: str}`` dicts.
        source_config: Quarter config the document was written for
                       (should contain ``quarter``, ``year``, ``quarter_label``).
        target_config: Target quarter config to roll forward to.
        edgar_facts:   Dict of XBRL fact name → value for the target period.
        api_key:       Anthropic API key.

    Returns:
        List of ``{index, original, suggestion, confidence}`` dicts in the
        same order as the input ``paragraphs`` list.
    """
    try:
        import anthropic
    except ImportError as exc:
        raise RuntimeError(
            "anthropic package not installed. Run: pip install anthropic"
        ) from exc

    client = anthropic.Anthropic(api_key=api_key)
    has_facts = bool(edgar_facts)
    output: list = [None] * len(paragraphs)

    # Process in batches
    for batch_start in range(0, len(paragraphs), _BATCH_SIZE):
        batch = paragraphs[batch_start : batch_start + _BATCH_SIZE]
        user_prompt = _build_user_prompt(batch, source_config, target_config, edgar_facts)

        try:
            message = client.messages.create(
                model=_MODEL,
                max_tokens=_MAX_TOKENS * len(batch),
                system=(
                    "You are a financial reporting assistant helping update SEC 10-Q "
                    "MD&A text. Be concise and precise."
                ),
                messages=[{"role": "user", "content": user_prompt}],
            )
            response_text = message.content[0].text if message.content else ""
            suggestions = _parse_batch_response(response_text, batch)

            for local_idx, para in enumerate(batch):
                global_idx = batch_start + local_idx
                confidence = "high" if has_facts else "medium"
                output[global_idx] = {
                    "index": para["index"],
                    "original": para["text"],
                    "suggestion": suggestions[local_idx],
                    "confidence": confidence,
                }

        except Exception as exc:  # anthropic.APIError and anything else
            logger.error("ai_assistant: API error for batch starting at %d: %s", batch_start, exc)
            for local_idx, para in enumerate(batch):
                global_idx = batch_start + local_idx
                output[global_idx] = {
                    "index": para["index"],
                    "original": para["text"],
                    "suggestion": "",
                    "confidence": "error",
                    "error": str(exc),
                }

    return output


def stream_mda_suggestion(
    paragraph: dict,
    source_config: dict,
    target_config: dict,
    edgar_facts: dict,
    api_key: str,
) -> Generator:
    """Stream a single MD&A paragraph suggestion from Claude API.

    Args:
        paragraph:     ``{index: int, text: str}`` dict.
        source_config: Source quarter config.
        target_config: Target quarter config.
        edgar_facts:   EDGAR XBRL fact dict for context.
        api_key:       Anthropic API key.

    Yields:
        Text chunks (str) as they are streamed from the API.

    Raises:
        RuntimeError: If the ``anthropic`` package is not installed.
    """
    try:
        import anthropic
    except ImportError as exc:
        raise RuntimeError(
            "anthropic package not installed. Run: pip install anthropic"
        ) from exc

    src_label = _quarter_label(source_config)
    tgt_label = _quarter_label(target_config)

    facts_summary = ""
    if edgar_facts:
        lines = [f"  {k}: {v}" for k, v in list(edgar_facts.items())[:20]]
        facts_summary = "\n".join(lines)
    else:
        facts_summary = "(no EDGAR facts available)"

    user_message = (
        f"Update this MD&A paragraph from {src_label} to {tgt_label}.\n\n"
        f"Original:\n{paragraph['text']}\n\n"
        f"Available EDGAR facts for {tgt_label}:\n{facts_summary}\n\n"
        "Provide ONLY the updated paragraph text."
    )

    client = anthropic.Anthropic(api_key=api_key)
    try:
        with client.messages.stream(
            model=_MODEL,
            max_tokens=_MAX_TOKENS,
            system=(
                "You are a financial reporting assistant helping update SEC 10-Q "
                "MD&A text. Be concise and precise."
            ),
            messages=[{"role": "user", "content": user_message}],
        ) as stream:
            for text_chunk in stream.text_stream:
                yield text_chunk
    except Exception as exc:
        logger.error("ai_assistant: streaming error for paragraph %d: %s", paragraph.get("index"), exc)
        yield f"[Error: {exc}]"
