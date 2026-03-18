"""
ingestion.py — Document ingestion pipeline: PDF/DOCX/HTML loaders, chunker,
source registry management. Feeds chunks into ChromaDB knowledge base.
"""

import os
import json
import glob
import hashlib
import logging
import time
from typing import Optional

from . import knowledge_base

logger = logging.getLogger(__name__)

# Registry lives next to this file
_REGISTRY_PATH = os.path.join(os.path.dirname(__file__), "source_registry.json")


# ---------------------------------------------------------------------------
# Registry helpers
# ---------------------------------------------------------------------------


def load_source_registry() -> dict:
    """Load the source registry from JSON.

    Returns:
        Dict mapping source_id → source metadata. Returns ``{}`` if file
        does not exist.
    """
    if not os.path.exists(_REGISTRY_PATH):
        return {}
    try:
        with open(_REGISTRY_PATH, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("ingestion: could not load registry: %s", exc)
        return {}


def save_source_registry(registry: dict) -> None:
    """Persist the source registry to JSON.

    Args:
        registry: Dict to serialise. Overwrites existing file.
    """
    try:
        with open(_REGISTRY_PATH, "w", encoding="utf-8") as fh:
            json.dump(registry, fh, indent=2)
    except OSError as exc:
        logger.error("ingestion: could not save registry: %s", exc)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def _compute_file_hash(path: str) -> str:
    """Compute a SHA-256 hex digest of a file (first 16 characters).

    Args:
        path: Absolute path to the file.

    Returns:
        First 16 characters of the SHA-256 hex digest.
    """
    sha = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(65536), b""):
            sha.update(block)
    return sha.hexdigest()[:16]


def chunk_text(text: str, chunk_size: int = 800, overlap: int = 150) -> list:
    """Split text into overlapping chunks using a word-based approximation.

    Uses ~4 characters per token as a rough heuristic so that
    ``chunk_size`` and ``overlap`` behave like token counts.

    Args:
        text:       Input text to split.
        chunk_size: Approximate maximum tokens per chunk.
        overlap:    Approximate token overlap between consecutive chunks.

    Returns:
        List of text chunk strings.
    """
    words = text.split()
    if not words:
        return []

    chars_per_chunk = chunk_size * 4
    chars_per_overlap = overlap * 4

    chunks = []
    current_chars = 0
    current_words: list = []
    overlap_buffer: list = []

    for word in words:
        current_words.extend(overlap_buffer)
        current_chars += sum(len(w) + 1 for w in overlap_buffer)
        overlap_buffer = []

        current_words.append(word)
        current_chars += len(word) + 1

        if current_chars >= chars_per_chunk:
            chunk_str = " ".join(current_words).strip()
            if chunk_str:
                chunks.append(chunk_str)

            # Build overlap from the tail of current_words
            tail = " ".join(current_words)
            overlap_text = tail[-chars_per_overlap:] if len(tail) > chars_per_overlap else tail
            overlap_buffer = overlap_text.split()

            current_words = []
            current_chars = 0

    # Remaining words
    remainder = " ".join(current_words).strip()
    if remainder:
        chunks.append(remainder)

    return chunks


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------


def load_pdf(path: str) -> list:
    """Extract text from a PDF using PyMuPDF (fitz).

    Args:
        path: Filesystem path to the PDF file.

    Returns:
        List of ``{page: int, text: str}`` dicts.

    Raises:
        RuntimeError: If PyMuPDF (fitz) is not installed.
    """
    try:
        import fitz  # PyMuPDF
    except ImportError:
        raise RuntimeError(
            "PyMuPDF not installed. Run: pip install pymupdf"
        )

    pages = []
    with fitz.open(path) as doc:
        for page_num, page in enumerate(doc, start=1):
            text = page.get_text().strip()
            if text:
                pages.append({"page": page_num, "text": text})

    logger.debug("load_pdf: extracted %d pages from %s", len(pages), path)
    return pages


def load_docx(path: str) -> list:
    """Extract text from a DOCX file using python-docx.

    Paragraphs are grouped into sections of ~20 paragraphs each.

    Args:
        path: Filesystem path to the DOCX file.

    Returns:
        List of ``{section: int, text: str}`` dicts.
    """
    try:
        from docx import Document
    except ImportError:
        raise RuntimeError(
            "python-docx not installed. Run: pip install python-docx"
        )

    doc = Document(path)
    paras = [p.text.strip() for p in doc.paragraphs if p.text.strip()]

    group_size = 20
    sections = []
    for i in range(0, len(paras), group_size):
        group = paras[i : i + group_size]
        sections.append(
            {"section": (i // group_size) + 1, "text": "\n".join(group)}
        )

    logger.debug("load_docx: extracted %d sections from %s", len(sections), path)
    return sections


def load_html(url: str) -> list:
    """Fetch and parse an HTML page, splitting on <h2>/<h3> boundaries.

    Args:
        url: URL to fetch.

    Returns:
        List of ``{section_title: str, text: str}`` dicts.

    Raises:
        RuntimeError: If BeautifulSoup4 is not installed.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        raise RuntimeError(
            "beautifulsoup4 not installed. Run: pip install beautifulsoup4 requests"
        )

    import requests

    logger.info("load_html: fetching %s", url)
    try:
        response = requests.get(url, timeout=30, headers={"User-Agent": "SEC-Compliance-KB/1.0"})
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to fetch {url}: {exc}") from exc

    soup = BeautifulSoup(response.text, "html.parser")

    # Remove script / style noise
    for tag in soup(["script", "style", "nav", "footer", "header"]):
        tag.decompose()

    sections = []
    current_title = "Introduction"
    current_texts: list = []

    for element in soup.find_all(["h2", "h3", "p", "li"]):
        tag = element.name
        if tag in ("h2", "h3"):
            if current_texts:
                sections.append(
                    {
                        "section_title": current_title,
                        "text": " ".join(current_texts).strip(),
                    }
                )
                current_texts = []
            current_title = element.get_text(separator=" ").strip()
        else:
            text = element.get_text(separator=" ").strip()
            if text:
                current_texts.append(text)

    # Flush last section
    if current_texts:
        sections.append(
            {
                "section_title": current_title,
                "text": " ".join(current_texts).strip(),
            }
        )

    logger.debug("load_html: extracted %d sections from %s", len(sections), url)
    return sections


# ---------------------------------------------------------------------------
# Main ingestion function
# ---------------------------------------------------------------------------


def ingest_source_file(
    source_id: str,
    source_name: str,
    location: str,
    source_type: str,
    collection,
    force: bool = False,
) -> dict:
    """Ingest a single source into the ChromaDB knowledge base.

    Checks the source registry to skip re-ingestion when the file hash is
    unchanged (unless ``force=True``).

    Args:
        source_id:   Short identifier string.
        source_name: Human-readable name.
        location:    File path or URL.
        source_type: One of ``"pdf"``, ``"docx"``, ``"html"``.
        collection:  ChromaDB collection from :func:`knowledge_base.get_collection`.
        force:       If ``True``, re-ingest even if hash is unchanged.

    Returns:
        Dict with keys: ``source_id``, ``chunks_ingested``, ``status``
        (``"ok"``, ``"skipped"``, or ``"error"``), ``message``.
    """
    registry = load_source_registry()

    # Compute file hash for local files (skip for URLs)
    file_hash: Optional[str] = None
    is_local = not location.startswith("http://") and not location.startswith("https://")
    resolved_location = os.path.expanduser(location) if is_local else location

    if is_local:
        if not os.path.exists(resolved_location):
            msg = f"File not found: {resolved_location}"
            logger.error("ingestion: %s", msg)
            return {"source_id": source_id, "chunks_ingested": 0, "status": "error", "message": msg}
        file_hash = _compute_file_hash(resolved_location)

    # Check if already ingested and up-to-date
    if not force and source_id in registry:
        existing = registry[source_id]
        if existing.get("status") == "ok" and (
            not is_local or existing.get("file_hash") == file_hash
        ):
            logger.info("ingestion: skipping '%s' (already up-to-date)", source_id)
            return {
                "source_id": source_id,
                "chunks_ingested": existing.get("chunk_count", 0),
                "status": "skipped",
                "message": "Already ingested and file unchanged.",
            }

    # Load content
    try:
        if source_type == "pdf":
            pages = load_pdf(resolved_location)
            raw_texts = [p["text"] for p in pages]
            metadatas_base = [{"page": p["page"]} for p in pages]
        elif source_type == "docx":
            sections = load_docx(resolved_location)
            raw_texts = [s["text"] for s in sections]
            metadatas_base = [{"section": s["section"]} for s in sections]
        elif source_type == "html":
            sections = load_html(resolved_location)
            raw_texts = [s["text"] for s in sections]
            metadatas_base = [{"section_title": s["section_title"]} for s in sections]
        else:
            return {
                "source_id": source_id,
                "chunks_ingested": 0,
                "status": "error",
                "message": f"Unknown source_type: {source_type}",
            }
    except Exception as exc:
        logger.error("ingestion: loading error for '%s': %s", source_id, exc)
        return {
            "source_id": source_id,
            "chunks_ingested": 0,
            "status": "error",
            "message": str(exc),
        }

    # Chunk
    all_chunks = []
    all_metadatas = []
    for raw_text, base_meta in zip(raw_texts, metadatas_base):
        for chunk in chunk_text(raw_text):
            all_chunks.append(chunk)
            all_metadatas.append(dict(base_meta))

    if not all_chunks:
        return {
            "source_id": source_id,
            "chunks_ingested": 0,
            "status": "error",
            "message": "No text extracted from source.",
        }

    # Remove existing chunks then upsert new ones
    knowledge_base.delete_source(collection, source_id)
    count = knowledge_base.ingest_source(
        collection, source_id, source_name, all_chunks, all_metadatas
    )

    # Update registry
    registry[source_id] = {
        "id": source_id,
        "name": source_name,
        "type": source_type,
        "location": location,
        "last_ingested": time.time(),
        "chunk_count": count,
        "file_hash": file_hash or "",
        "status": "ok",
    }
    save_source_registry(registry)

    logger.info("ingestion: ingested %d chunks for '%s'", count, source_id)
    return {
        "source_id": source_id,
        "chunks_ingested": count,
        "status": "ok",
        "message": f"Successfully ingested {count} chunks.",
    }


# ---------------------------------------------------------------------------
# Default source list
# ---------------------------------------------------------------------------


def get_default_sources() -> list:
    """Return the predefined list of regulatory and practitioner sources.

    Local paths use ``os.path.expanduser("~")`` to resolve ``~``.

    Returns:
        List of source dicts with keys: ``id``, ``name``, ``type``,
        ``location``.
    """
    sources = [
        {
            "id": "form_10k_inst",
            "name": "SEC Form 10-K Instructions",
            "type": "pdf",
            "location": "https://www.sec.gov/files/form10-k.pdf",
        },
        {
            "id": "form_10q_inst",
            "name": "SEC Form 10-Q Instructions",
            "type": "pdf",
            "location": "https://www.sec.gov/files/form10-q.pdf",
        },
        {
            "id": "reg_sk",
            "name": "Regulation S-K",
            "type": "html",
            "location": "https://www.ecfr.gov/current/title-17/chapter-II/part-229",
        },
        {
            "id": "reg_sx",
            "name": "Regulation S-X",
            "type": "html",
            "location": "https://www.ecfr.gov/current/title-17/chapter-II/part-210",
        },
        {
            "id": "sec_frm",
            "name": "SEC Financial Reporting Manual",
            "type": "pdf",
            "location": "https://www.sec.gov/files/cf-frm.pdf",
        },
        {
            "id": "pwc_fsp",
            "name": "PWC FS Presentation 2026",
            "type": "pdf",
            "location": os.path.join(
                os.path.expanduser("~"),
                "OneDrive",
                "Business",
                "Guides and publishings",
                "PWC FS Presentation 2026.pdf",
            ),
        },
        {
            "id": "disclosure_ck",
            "name": "Disclosure Checklist",
            "type": "docx",
            "location": os.path.join(
                os.path.expanduser("~"),
                "OneDrive",
                "Business",
                "Guides and publishings",
                "Disclosure Checklist.docx",
            ),
        },
        {
            "id": "sox_ck",
            "name": "SOX Checklist Q Dec 2025",
            "type": "docx",
            "location": os.path.join(
                os.path.expanduser("~"),
                "OneDrive",
                "Business",
                "Guides and publishings",
                "217.Q SOX_Checklist December 31 2025.docx",
            ),
        },
        {
            "id": "interim_fsdc",
            "name": "Interim FSDC Checklist",
            "type": "docx",
            "location": os.path.join(
                os.path.expanduser("~"),
                "OneDrive",
                "Business",
                "Guides and publishings",
                "216.Q_INTERIM_FSDC_12-31-25.docx",
            ),
        },
    ]

    # Dynamically discover Deloitte Roadmap PDFs
    deloitte_pattern = os.path.join(
        os.path.expanduser("~"),
        "OneDrive - JT",
        "Documents",
        "Deloitte Roadmap Series",
        "*.pdf",
    )
    for pdf_path in sorted(glob.glob(deloitte_pattern)):
        base = os.path.splitext(os.path.basename(pdf_path))[0]
        safe_id = "deloitte_" + base.lower().replace(" ", "_")[:40]
        sources.append(
            {
                "id": safe_id,
                "name": f"Deloitte Roadmap: {base}",
                "type": "pdf",
                "location": pdf_path,
            }
        )

    return sources
