"""
knowledge_base.py — ChromaDB vector store wrapper for SEC compliance knowledge base.
Stores regulatory documents (SEC rules, GAAP standards, practitioner guides) as
embedded chunks for semantic search and RAG.
"""

import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Default persistence directory relative to this file's location
_DEFAULT_PERSIST_DIR = os.path.join(os.path.dirname(__file__), "..", "compliance_db")

_COLLECTION_NAME = "sec_compliance_kb"


def _require_chromadb():
    """Import chromadb or raise a clear RuntimeError."""
    try:
        import chromadb  # noqa: F401
        return chromadb
    except ImportError:
        raise RuntimeError("chromadb not installed. Run: pip install chromadb")


def get_chroma_client(persist_dir: Optional[str] = None):
    """Create and return a ChromaDB client.

    Args:
        persist_dir: Directory path for persistent storage. Defaults to
                     ``compliance_db`` in the sec_rollforward root.

    Returns:
        A ``chromadb.PersistentClient`` instance.
    """
    chromadb = _require_chromadb()

    if persist_dir is None:
        persist_dir = os.path.abspath(_DEFAULT_PERSIST_DIR)

    os.makedirs(persist_dir, exist_ok=True)
    logger.debug("knowledge_base: using ChromaDB at %s", persist_dir)
    return chromadb.PersistentClient(path=persist_dir)


def get_collection(client):
    """Get or create the SEC compliance knowledge base collection.

    Uses the default ChromaDB embedding function (``all-MiniLM-L6-v2``
    via sentence-transformers when available, otherwise a simple fallback).

    Args:
        client: A ``chromadb.Client`` instance returned by
                :func:`get_chroma_client`.

    Returns:
        A ``chromadb.Collection`` object.
    """
    collection = client.get_or_create_collection(
        name=_COLLECTION_NAME,
        metadata={"description": "SEC compliance regulatory knowledge base"},
    )
    logger.debug(
        "knowledge_base: collection '%s' ready (%d chunks)",
        _COLLECTION_NAME,
        collection.count(),
    )
    return collection


def ingest_source(
    collection,
    source_id: str,
    source_name: str,
    chunks: list,
    metadatas: list,
) -> int:
    """Upsert text chunks into the collection.

    Each chunk receives an id of the form ``{source_id}_chunk_{i}``.
    The supplied metadata dicts are merged with ``source_id``,
    ``source_name``, and ``chunk_index`` fields.

    Args:
        collection:  ChromaDB collection.
        source_id:   Short identifier for the source (e.g. ``"reg_sk"``).
        source_name: Human-readable name (e.g. ``"Regulation S-K"``).
        chunks:      List of text strings to upsert.
        metadatas:   Parallel list of metadata dicts (same length as chunks).

    Returns:
        Number of chunks upserted.
    """
    if not chunks:
        return 0

    ids = []
    docs = []
    metas = []

    for i, (chunk, meta) in enumerate(zip(chunks, metadatas)):
        ids.append(f"{source_id}_chunk_{i}")
        docs.append(chunk)
        merged = dict(meta)
        merged.update(
            {
                "source_id": source_id,
                "source_name": source_name,
                "chunk_index": i,
            }
        )
        metas.append(merged)

    # ChromaDB upsert in batches of 500 to avoid payload limits
    batch_size = 500
    for start in range(0, len(ids), batch_size):
        collection.upsert(
            ids=ids[start : start + batch_size],
            documents=docs[start : start + batch_size],
            metadatas=metas[start : start + batch_size],
        )

    logger.info(
        "knowledge_base: upserted %d chunks for source '%s'", len(ids), source_id
    )
    return len(ids)


def search(
    collection,
    query: str,
    n_results: int = 8,
    filter_sources: Optional[list] = None,
) -> list:
    """Semantic search over the knowledge base.

    Args:
        collection:     ChromaDB collection.
        query:          Natural-language query string.
        n_results:      Number of top results to return.
        filter_sources: Optional list of ``source_id`` strings to restrict
                        the search to.

    Returns:
        List of dicts with keys: ``text``, ``source_id``, ``source_name``,
        ``metadata``, ``distance``.
    """
    where = None
    if filter_sources:
        if len(filter_sources) == 1:
            where = {"source_id": {"$eq": filter_sources[0]}}
        else:
            where = {"source_id": {"$in": filter_sources}}

    query_kwargs = dict(
        query_texts=[query],
        n_results=min(n_results, max(collection.count(), 1)),
    )
    if where:
        query_kwargs["where"] = where

    try:
        results = collection.query(**query_kwargs)
    except Exception as exc:
        logger.error("knowledge_base: search error: %s", exc)
        return []

    output = []
    docs = results.get("documents", [[]])[0]
    metas = results.get("metadatas", [[]])[0]
    distances = results.get("distances", [[]])[0]

    for doc, meta, dist in zip(docs, metas, distances):
        output.append(
            {
                "text": doc,
                "source_id": meta.get("source_id", ""),
                "source_name": meta.get("source_name", ""),
                "metadata": meta,
                "distance": dist,
            }
        )

    return output


def get_status(collection) -> dict:
    """Return statistics about the knowledge base.

    Args:
        collection: ChromaDB collection.

    Returns:
        Dict with ``total_chunks`` (int) and ``sources``
        (dict mapping source_id → chunk count).
    """
    total = collection.count()
    sources: dict = {}

    if total > 0:
        try:
            # Fetch all metadatas (no documents, to keep it light)
            all_meta = collection.get(include=["metadatas"])
            for meta in all_meta.get("metadatas", []):
                sid = meta.get("source_id", "unknown")
                sources[sid] = sources.get(sid, 0) + 1
        except Exception as exc:
            logger.warning("knowledge_base: could not enumerate sources: %s", exc)

    return {"total_chunks": total, "sources": sources}


def delete_source(collection, source_id: str) -> int:
    """Delete all chunks for a given source from the collection.

    Args:
        collection: ChromaDB collection.
        source_id:  Source identifier whose chunks should be removed.

    Returns:
        Number of chunks deleted.
    """
    try:
        existing = collection.get(
            where={"source_id": {"$eq": source_id}},
            include=[],
        )
        chunk_ids = existing.get("ids", [])
        if chunk_ids:
            collection.delete(ids=chunk_ids)
            logger.info(
                "knowledge_base: deleted %d chunks for source '%s'",
                len(chunk_ids),
                source_id,
            )
        return len(chunk_ids)
    except Exception as exc:
        logger.error(
            "knowledge_base: error deleting source '%s': %s", source_id, exc
        )
        return 0
