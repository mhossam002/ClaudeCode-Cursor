"""
chatbot.py — Claude API + RAG chatbot for SEC compliance Q&A.
Uses ChromaDB semantic search to retrieve relevant regulatory context,
then streams Claude API responses with inline citations.
"""

import json
import logging
import re
from typing import Generator, Optional

from . import knowledge_base as kb

logger = logging.getLogger(__name__)

_MODEL = "claude-haiku-4-5-20251001"
_MAX_TOKENS = 1024
_MAX_HISTORY = 10  # number of prior turns to include

_SYSTEM_PROMPT = (
    "You are an expert SEC compliance advisor with deep knowledge of US GAAP, "
    "SEC regulations (Reg S-K, Reg S-X), and financial reporting standards. "
    "Always cite your sources using [Source Name - Section] notation. "
    "Be precise and practical."
)


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------


def build_rag_prompt(
    query: str,
    retrieved_chunks: list,
    chat_history: Optional[list] = None,
) -> list:
    """Build a messages list for the Claude API, incorporating RAG context.

    Args:
        query:            User's question.
        retrieved_chunks: List of chunk dicts from :func:`knowledge_base.search`.
        chat_history:     Optional list of prior ``{role, content}`` dicts.
                          Only the last :data:`_MAX_HISTORY` turns are kept.

    Returns:
        A list of ``{role: str, content: str}`` message dicts suitable for
        ``client.messages.create(messages=...)``.
    """
    messages: list = []

    # RAG context block as the first user turn
    if retrieved_chunks:
        context_lines = []
        for i, chunk in enumerate(retrieved_chunks, 1):
            src = chunk.get("source_name", chunk.get("source_id", "Unknown"))
            text = chunk.get("text", "")
            context_lines.append(f"[{i}] {src}:\n{text}")
        context_text = "REGULATORY CONTEXT:\n\n" + "\n\n".join(context_lines)
        messages.append({"role": "user", "content": context_text})
        messages.append(
            {
                "role": "assistant",
                "content": "I have reviewed the regulatory context. Please ask your question.",
            }
        )

    # Append trimmed chat history
    if chat_history:
        trimmed = chat_history[-(_MAX_HISTORY * 2):]  # keep last N full turns
        for turn in trimmed:
            if isinstance(turn, dict) and "role" in turn and "content" in turn:
                messages.append({"role": turn["role"], "content": turn["content"]})

    # Append the current query
    messages.append({"role": "user", "content": query})

    return messages


# ---------------------------------------------------------------------------
# Citation parser
# ---------------------------------------------------------------------------


def _parse_citations(text: str) -> list:
    """Extract ``[Source Name - Section]`` style citations from response text.

    Args:
        text: Claude response text.

    Returns:
        List of dicts with ``citation`` and ``raw`` keys.
    """
    pattern = r"\[([^\[\]]+)\]"
    citations = []
    seen: set = set()
    for match in re.finditer(pattern, text):
        raw = match.group(0)
        inner = match.group(1).strip()
        if inner and raw not in seen:
            seen.add(raw)
            citations.append({"citation": inner, "raw": raw})
    return citations


# ---------------------------------------------------------------------------
# Non-streaming chat
# ---------------------------------------------------------------------------


def chat(
    query: str,
    collection,
    api_key: str,
    chat_history: Optional[list] = None,
    n_results: int = 8,
) -> dict:
    """Answer a compliance question using RAG + Claude API (non-streaming).

    Args:
        query:        User question.
        collection:   ChromaDB collection.
        api_key:      Anthropic API key.
        chat_history: Optional prior conversation turns.
        n_results:    Number of knowledge-base chunks to retrieve.

    Returns:
        Dict with keys: ``response`` (str), ``citations`` (list),
        ``retrieved_chunks`` (list).
    """
    try:
        import anthropic
    except ImportError:
        raise RuntimeError("anthropic package not installed. Run: pip install anthropic")

    # Semantic search
    chunks = kb.search(collection, query, n_results=n_results)

    # Build messages
    messages = build_rag_prompt(query, chunks, chat_history)

    client = anthropic.Anthropic(api_key=api_key)
    try:
        response = client.messages.create(
            model=_MODEL,
            max_tokens=_MAX_TOKENS,
            system=_SYSTEM_PROMPT,
            messages=messages,
        )
        response_text = response.content[0].text if response.content else ""
    except Exception as exc:
        logger.error("chatbot.chat: API error: %s", exc)
        return {
            "response": f"Error: {exc}",
            "citations": [],
            "retrieved_chunks": chunks,
        }

    citations = _parse_citations(response_text)
    return {
        "response": response_text,
        "citations": citations,
        "retrieved_chunks": chunks,
    }


# ---------------------------------------------------------------------------
# Streaming chat
# ---------------------------------------------------------------------------


def stream_chat(
    query: str,
    collection,
    api_key: str,
    chat_history: Optional[list] = None,
    n_results: int = 8,
) -> Generator:
    """Stream a compliance answer via Server-Sent Events (SSE).

    Yields SSE-formatted strings. Two event types are emitted:

    * ``{"type": "chunk", "text": "..."}`` — incremental text tokens.
    * ``{"type": "done", "citations": [...]}`` — emitted once after the
      stream completes, containing parsed citations.

    Args:
        query:        User question.
        collection:   ChromaDB collection.
        api_key:      Anthropic API key.
        chat_history: Optional prior conversation turns.
        n_results:    Number of knowledge-base chunks to retrieve.

    Yields:
        SSE-formatted strings (``data: {...}\\n\\n``).
    """
    try:
        import anthropic
    except ImportError:
        err = json.dumps({"type": "error", "text": "anthropic not installed"})
        yield f"data: {err}\n\n"
        return

    chunks = kb.search(collection, query, n_results=n_results)
    messages = build_rag_prompt(query, chunks, chat_history)

    client = anthropic.Anthropic(api_key=api_key)
    full_text = ""

    try:
        with client.messages.stream(
            model=_MODEL,
            max_tokens=_MAX_TOKENS,
            system=_SYSTEM_PROMPT,
            messages=messages,
        ) as stream:
            for text_chunk in stream.text_stream:
                full_text += text_chunk
                payload = json.dumps({"type": "chunk", "text": text_chunk})
                yield f"data: {payload}\n\n"
    except Exception as exc:
        logger.error("chatbot.stream_chat: streaming error: %s", exc)
        err_payload = json.dumps({"type": "error", "text": str(exc)})
        yield f"data: {err_payload}\n\n"
        return

    citations = _parse_citations(full_text)
    done_payload = json.dumps({"type": "done", "citations": citations})
    yield f"data: {done_payload}\n\n"
