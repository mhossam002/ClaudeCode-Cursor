"""
app.py — Flask web application for SEC 10-Q Roll-Forward Tool.
"""
import os
import uuid
import logging
import tempfile
import json
import queue
import threading
import time

from flask import (
    Flask, render_template, request, jsonify,
    send_file, Response, stream_with_context,
)

from rollforward.docx_parser import load_document, extract_table_map
from rollforward.edgar_client import (
    fetch_company_facts, build_fact_lookup, detect_period_config,
    lookup_ticker, build_filing_url, list_available_filings,
)
from rollforward.engine import roll_forward, DJCO_CIK, Q1_CONFIG, Q2_CONFIG
from rollforward.session_store import (
    init_db, add_session, get_session, touch_session, add_output,
    list_sessions, delete_session as _delete_session, cleanup_expired,
    _DEFAULT_DB_PATH,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = "SEC Roll-Forward Tool sec-rollforward@example.com"

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB

# In-memory fast lookup {file_id: abs_path} (also persisted in SQLite)
_uploaded_files: dict = {}

# Job store for SSE-based roll-forward {job_id: {output_path, output_filename, stats}}
_jobs: dict = {}

# HTML session store {html_id: {html, url, replacements}}
_html_sessions: dict = {}

UPLOAD_DIR = os.path.join(tempfile.gettempdir(), "sec_rollforward_uploads")
OUTPUT_DIR = os.path.join(tempfile.gettempdir(), "sec_rollforward_outputs")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Initialize SQLite session store
_DB_PATH = _DEFAULT_DB_PATH
init_db(_DB_PATH)

# Anthropic API key from environment (user can also supply per-request)
_ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")


# Background cleanup thread — runs every hour
def _cleanup_worker():
    while True:
        time.sleep(3600)
        try:
            deleted = cleanup_expired(max_age_seconds=86400, db_path=_DB_PATH)
            if deleted:
                logger.info("Session cleanup: removed %d expired sessions", len(deleted))
                for sid in deleted:
                    _uploaded_files.pop(sid, None)
        except Exception as exc:
            logger.warning("Session cleanup error: %s", exc)

_cleanup_thread = threading.Thread(target=_cleanup_worker, daemon=True)
_cleanup_thread.start()


# ---------------------------------------------------------------------------
# Lazy compliance module loader
# ---------------------------------------------------------------------------

_chroma_client = None
_chroma_collection = None


def _get_compliance_collection():
    global _chroma_client, _chroma_collection
    if _chroma_collection is None:
        from compliance.knowledge_base import get_chroma_client, get_collection
        _chroma_client = get_chroma_client()
        _chroma_collection = get_collection(_chroma_client)
    return _chroma_collection


# ---------------------------------------------------------------------------
# Core routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/compliance")
def compliance_admin():
    return render_template("compliance_admin.html")


@app.route("/upload", methods=["POST"])
def upload():
    """
    POST /upload
    Returns: {file_id, filename, table_map}
    """
    if "file" not in request.files:
        return jsonify({"error": "No file field in request"}), 400

    f = request.files["file"]
    if not f.filename.endswith(".docx"):
        return jsonify({"error": "Only .docx files are accepted"}), 400

    file_id   = str(uuid.uuid4())
    dest_path = os.path.join(UPLOAD_DIR, f"{file_id}.docx")
    f.save(dest_path)
    _uploaded_files[file_id] = dest_path

    try:
        add_session(file_id, dest_path, f.filename, _DB_PATH)
    except Exception as exc:
        logger.warning("session_store.add_session failed: %s", exc)

    try:
        doc       = load_document(dest_path)
        table_map = extract_table_map(doc)
    except Exception as exc:
        logger.exception("Failed to parse uploaded document")
        return jsonify({"error": f"Failed to parse document: {exc}"}), 500

    return jsonify({"file_id": file_id, "filename": f.filename, "table_map": table_map})


@app.route("/api/lookup-ticker")
def api_lookup_ticker():
    """GET /api/lookup-ticker?ticker=DJCO"""
    ticker = request.args.get("ticker", "").strip()
    if not ticker:
        return jsonify({"error": "ticker parameter required"}), 400
    try:
        result = lookup_ticker(ticker, DEFAULT_USER_AGENT)
        return jsonify(result)
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/list-filings")
def list_filings():
    """Return available 10-Q/10-K filings for a CIK."""
    cik        = request.args.get("cik", "").strip()
    user_agent = request.args.get("user_agent", DEFAULT_USER_AGENT).strip()
    if not cik:
        return jsonify({"error": "cik required"}), 400
    try:
        filings = list_available_filings(cik, user_agent)
        return jsonify({"filings": filings})
    except Exception as exc:
        logger.exception("list-filings failed")
        return jsonify({"error": str(exc)}), 500


@app.route("/api/detect-period")
def api_detect_period():
    """
    GET /api/detect-period?cik=...&user_agent=...&form_type=10-Q
    """
    cik        = request.args.get("cik", DJCO_CIK).strip()
    user_agent = request.args.get("user_agent", "").strip()
    form_type  = request.args.get("form_type", "10-Q").strip().upper()

    if not user_agent:
        return jsonify({"error": "user_agent is required"}), 400
    try:
        result = detect_period_config(cik, user_agent, form_type=form_type)
    except Exception as exc:
        logger.exception("Period detection failed")
        return jsonify({"error": str(exc)}), 502
    return jsonify(result)


def _build_output_filename(target_config, id_fragment):
    if target_config and target_config.get("period_label"):
        safe = (target_config["period_label"]
                .replace(" ", "_").replace(",", "").replace("/", "-"))
        return f"Form_10-Q_{safe}_draft_{id_fragment}.docx"
    return f"Form_10-Q_draft_{id_fragment}.docx"


@app.route("/rollforward", methods=["POST"])
def do_rollforward():
    """
    POST /rollforward — Returns output .docx as file download.
    Stats are embedded in X-Rollforward-Stats response header (JSON).
    """
    data          = request.get_json(force=True, silent=True) or {}
    file_id       = data.get("file_id")
    cik           = data.get("cik", DJCO_CIK).strip()
    user_agent    = data.get("user_agent", "DJCO SEC Tool admin@example.com").strip()
    add_ytd       = bool(data.get("add_ytd", False))
    source_config = data.get("source_config") or None
    target_config = data.get("target_config") or None

    if not file_id or file_id not in _uploaded_files:
        return jsonify({"error": "Invalid or expired file_id. Please re-upload."}), 400

    source_path     = _uploaded_files[file_id]
    output_filename = _build_output_filename(target_config, file_id[:8])
    output_path     = os.path.join(OUTPUT_DIR, output_filename)

    try:
        stats = roll_forward(
            source_path=source_path,
            output_path=output_path,
            cik=cik,
            user_agent=user_agent,
            add_ytd=add_ytd,
            source_config=source_config,
            target_config=target_config,
        )
    except Exception as exc:
        logger.exception("Roll-forward failed")
        return jsonify({"error": f"Roll-forward failed: {exc}"}), 500

    if stats.get("status") != "ok":
        return jsonify({"error": "Roll-forward returned non-ok status", "stats": stats}), 500

    try:
        add_output(str(uuid.uuid4()), file_id, output_path, output_filename, _DB_PATH)
    except Exception:
        pass

    resp = send_file(
        output_path,
        as_attachment=True,
        download_name=output_filename,
        mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )
    stats_summary = {
        "replacements_made":   stats.get("replacements_made"),
        "tables_processed":    stats.get("tables_processed"),
        "edgar_facts_found":   stats.get("edgar_facts_found"),
        "edgar_facts_missing": stats.get("edgar_facts_missing", []),
        "warnings":            stats.get("warnings", []),
        "verification":        stats.get("verification", {}),
        "changes":             stats.get("changes", []),
        "table_detection":     {k: v for k, v in stats.get("table_detection", {}).items()
                                if k != "detected_at"},
    }
    try:
        resp.headers["X-Rollforward-Stats"] = json.dumps(stats_summary)[:4096]
        resp.headers["Access-Control-Expose-Headers"] = "X-Rollforward-Stats"
    except Exception:
        pass
    return resp


@app.route("/rollforward-stream", methods=["POST"])
def rollforward_stream():
    """
    POST /rollforward-stream
    SSE stream of progress events.
    Final event: {type:"done", job_id:"...", stats:{...}}
    Download via GET /download/<job_id>
    """
    data          = request.get_json(force=True, silent=True) or {}
    file_id       = data.get("file_id")
    cik           = data.get("cik", DJCO_CIK).strip()
    user_agent    = data.get("user_agent", "DJCO SEC Tool admin@example.com").strip()
    add_ytd       = bool(data.get("add_ytd", False))
    source_config = data.get("source_config") or None
    target_config = data.get("target_config") or None

    if not file_id or file_id not in _uploaded_files:
        return jsonify({"error": "Invalid or expired file_id. Please re-upload."}), 400

    source_path     = _uploaded_files[file_id]
    job_id          = str(uuid.uuid4())
    output_filename = _build_output_filename(target_config, job_id[:8])
    output_path     = os.path.join(OUTPUT_DIR, output_filename)

    progress_q = queue.Queue()

    def progress_callback(msg: str):
        progress_q.put({"type": "progress", "message": msg})

    def worker():
        try:
            stats = roll_forward(
                source_path=source_path,
                output_path=output_path,
                cik=cik,
                user_agent=user_agent,
                add_ytd=add_ytd,
                source_config=source_config,
                target_config=target_config,
                progress_callback=progress_callback,
            )
            _jobs[job_id] = {
                "output_path":     output_path,
                "output_filename": output_filename,
                "stats":           stats,
                "file_id":         file_id,
            }
            try:
                add_output(str(uuid.uuid4()), file_id, output_path, output_filename, _DB_PATH)
            except Exception:
                pass
            progress_q.put({
                "type":    "done",
                "job_id":  job_id,
                "stats": {
                    "replacements_made":   stats.get("replacements_made"),
                    "tables_processed":    stats.get("tables_processed"),
                    "edgar_facts_found":   stats.get("edgar_facts_found"),
                    "edgar_facts_missing": stats.get("edgar_facts_missing", []),
                    "warnings":            stats.get("warnings", []),
                    "verification":        stats.get("verification", {}),
                    "changes":             stats.get("changes", []),
                    "table_detection":     {k: v for k, v in stats.get("table_detection", {}).items()
                                           if k != "detected_at"},
                },
            })
        except Exception as exc:
            progress_q.put({"type": "error", "message": str(exc)})

    threading.Thread(target=worker, daemon=True).start()

    def generate():
        while True:
            try:
                event = progress_q.get(timeout=90)
                yield f"data: {json.dumps(event)}\n\n"
                if event.get("type") in ("done", "error"):
                    break
            except queue.Empty:
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"

    return Response(
        stream_with_context(generate()),
        content_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/download/<job_id>")
def download_job(job_id):
    """GET /download/<job_id> — Download output from a completed SSE roll-forward job."""
    job = _jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found or expired"}), 404
    output_path = job["output_path"]
    if not os.path.exists(output_path):
        return jsonify({"error": "Output file not found"}), 404
    return send_file(
        output_path,
        as_attachment=True,
        download_name=job.get("output_filename", "Form_10-Q_draft.docx"),
        mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )


@app.route("/edgar-preview")
def edgar_preview():
    """GET /edgar-preview?cik=...&period_end=...&user_agent=..."""
    cik        = request.args.get("cik", DJCO_CIK).strip()
    period_end = request.args.get("period_end", Q2_CONFIG["comparable_end"]).strip()
    user_agent = request.args.get("user_agent", "DJCO SEC Tool admin@example.com").strip()

    try:
        facts_json = fetch_company_facts(cik, user_agent)
        lookup     = build_fact_lookup(facts_json)
    except Exception as exc:
        return jsonify({"error": f"EDGAR fetch failed: {exc}"}), 502

    preview = {
        f"{concept}/{months}mo": val
        for (concept, end, months), val in lookup.items()
        if end == period_end
    }
    return jsonify({"cik": cik, "period_end": period_end, "facts": preview, "count": len(preview)})


@app.route("/stats/<file_id>")
def get_stats(file_id):
    return jsonify({"error": "Stats not persisted. Check X-Rollforward-Stats header."}), 404


# ---------------------------------------------------------------------------
# AI MD&A
# ---------------------------------------------------------------------------

@app.route("/api/ai/mda-suggestions", methods=["POST"])
def api_mda_suggestions():
    """
    POST /api/ai/mda-suggestions
    Body: {paragraphs:[{index,text}], source_config, target_config, edgar_facts, api_key?}
    """
    data          = request.get_json(force=True, silent=True) or {}
    paragraphs    = data.get("paragraphs", [])
    source_config = data.get("source_config", {})
    target_config = data.get("target_config", {})
    edgar_facts   = data.get("edgar_facts", {})
    api_key       = data.get("api_key") or _ANTHROPIC_API_KEY

    if not api_key:
        return jsonify({"error": "Anthropic API key required. Pass api_key in request or set ANTHROPIC_API_KEY env var."}), 400
    if not paragraphs:
        return jsonify({"suggestions": []})

    try:
        from rollforward.ai_assistant import get_mda_suggestions
        suggestions = get_mda_suggestions(paragraphs, source_config, target_config, edgar_facts, api_key)
        return jsonify({"suggestions": suggestions})
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 503
    except Exception as exc:
        logger.exception("MD&A suggestions failed")
        return jsonify({"error": f"AI suggestion failed: {exc}"}), 500


@app.route("/api/ai/mda-stream", methods=["POST"])
def api_mda_stream():
    """
    POST /api/ai/mda-stream
    Body: {paragraph:{index,text}, source_config, target_config, edgar_facts, api_key?}
    SSE stream of suggestion text chunks.
    """
    data          = request.get_json(force=True, silent=True) or {}
    paragraph     = data.get("paragraph", {})
    source_config = data.get("source_config", {})
    target_config = data.get("target_config", {})
    edgar_facts   = data.get("edgar_facts", {})
    api_key       = data.get("api_key") or _ANTHROPIC_API_KEY

    if not api_key:
        return jsonify({"error": "Anthropic API key required"}), 400

    try:
        from rollforward.ai_assistant import stream_mda_suggestion

        def generate():
            for chunk in stream_mda_suggestion(paragraph, source_config, target_config, edgar_facts, api_key):
                yield f"data: {json.dumps({'type': 'chunk', 'text': chunk})}\n\n"
            yield f"data: {json.dumps({'type': 'done'})}\n\n"

        return Response(
            stream_with_context(generate()),
            content_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 503
    except Exception as exc:
        logger.exception("MD&A stream failed")
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

@app.route("/api/sessions")
def api_sessions():
    try:
        sessions = list_sessions(db_path=_DB_PATH)
        return jsonify({"sessions": sessions, "count": len(sessions)})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/sessions/<session_id>", methods=["DELETE"])
def api_delete_session(session_id):
    try:
        _delete_session(session_id, db_path=_DB_PATH)
        _uploaded_files.pop(session_id, None)
        return jsonify({"deleted": session_id})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Compliance Intelligence Layer
# ---------------------------------------------------------------------------

@app.route("/api/compliance/status")
def api_compliance_status():
    """GET /api/compliance/status — Vector store stats + source registry."""
    try:
        from compliance.knowledge_base import get_status
        from compliance.ingestion import load_source_registry, get_default_sources
        import glob

        collection = _get_compliance_collection()
        kb_status  = get_status(collection)
        registry   = load_source_registry()
        sources    = get_default_sources()

        source_rows = []
        for src in sources:
            reg = registry.get(src["id"], {})
            source_rows.append({
                "id":            src["id"],
                "name":          src["name"],
                "type":          src["type"],
                "status":        reg.get("status", "pending"),
                "chunk_count":   kb_status["sources"].get(src["id"], 0),
                "last_ingested": reg.get("last_ingested"),
                "location":      src["location"],
            })

        # Discover Deloitte PDFs
        deloitte_dir  = os.path.expanduser(
            "~/OneDrive - JT/Documents/Deloitte Roadmap Series/")
        deloitte_pdfs = sorted(glob.glob(os.path.join(deloitte_dir, "*.pdf")))[:30]
        for pdf_path in deloitte_pdfs:
            src_id = ("deloitte_" + os.path.basename(pdf_path)
                      .replace(" ", "_").replace(".pdf", ""))[:40]
            reg = registry.get(src_id, {})
            source_rows.append({
                "id":            src_id,
                "name":          os.path.basename(pdf_path),
                "type":          "pdf",
                "status":        reg.get("status", "pending"),
                "chunk_count":   kb_status["sources"].get(src_id, 0),
                "last_ingested": reg.get("last_ingested"),
                "location":      pdf_path,
            })

        db_path    = os.path.join(os.path.dirname(__file__), "compliance_db")
        db_size_mb = 0
        if os.path.exists(db_path):
            total = sum(
                os.path.getsize(os.path.join(dp, fn))
                for dp, _, files in os.walk(db_path)
                for fn in files
            )
            db_size_mb = round(total / 1024 / 1024, 1)

        return jsonify({
            "total_chunks":    kb_status["total_chunks"],
            "sources":         source_rows,
            "db_size_mb":      db_size_mb,
            "embedding_model": "all-MiniLM-L6-v2 (ChromaDB default)",
            "collection_name": "sec_compliance_kb",
        })
    except RuntimeError as exc:
        return jsonify({
            "error": str(exc), "total_chunks": 0,
            "sources": [], "db_size_mb": 0,
        }), 503
    except Exception as exc:
        logger.exception("Compliance status failed")
        return jsonify({"error": str(exc)}), 500


@app.route("/api/compliance/ingest", methods=["POST"])
def api_compliance_ingest():
    """
    POST /api/compliance/ingest
    Body: {source_id?: str}
    """
    data      = request.get_json(force=True, silent=True) or {}
    source_id = data.get("source_id")

    try:
        from compliance.ingestion import get_default_sources, ingest_source_file

        collection = _get_compliance_collection()
        sources    = get_default_sources()
        if source_id:
            sources = [s for s in sources if s["id"] == source_id]
            if not sources:
                return jsonify({"error": f"Source '{source_id}' not found"}), 404

        results = []
        for src in sources:
            result = ingest_source_file(
                src["id"], src["name"], src["location"], src["type"],
                collection, force=(source_id is not None),
            )
            results.append(result)

        return jsonify({"results": results})
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 503
    except Exception as exc:
        logger.exception("Compliance ingest failed")
        return jsonify({"error": str(exc)}), 500


@app.route("/api/compliance/ingest-stream")
def api_compliance_ingest_stream():
    """GET /api/compliance/ingest-stream?source_id=... — SSE ingestion progress."""
    source_id  = request.args.get("source_id")
    progress_q = queue.Queue()

    def ingest_worker():
        try:
            from compliance.ingestion import get_default_sources, ingest_source_file
            import glob

            collection = _get_compliance_collection()
            sources    = get_default_sources()

            # Add Deloitte PDFs
            deloitte_dir  = os.path.expanduser(
                "~/OneDrive - JT/Documents/Deloitte Roadmap Series/")
            deloitte_pdfs = sorted(glob.glob(os.path.join(deloitte_dir, "*.pdf")))[:30]
            for pdf_path in deloitte_pdfs:
                src_id = ("deloitte_" + os.path.basename(pdf_path)
                          .replace(" ", "_").replace(".pdf", ""))[:40]
                sources.append({
                    "id":       src_id,
                    "name":     os.path.basename(pdf_path),
                    "type":     "pdf",
                    "location": pdf_path,
                })

            if source_id:
                sources = [s for s in sources if s["id"] == source_id]

            total = len(sources)
            for i, src in enumerate(sources, 1):
                progress_q.put({"type": "progress",
                                 "message": f"[{i}/{total}] Ingesting: {src['name']}..."})
                try:
                    result = ingest_source_file(
                        src["id"], src["name"], src["location"], src["type"],
                        collection, force=(source_id is not None),
                    )
                    if result["status"] == "ok":
                        progress_q.put({"type": "done_source", "source_id": src["id"],
                                        "message": f"  ✓ {src['name']}: {result['chunks_ingested']} chunks"})
                    elif result["status"] == "skipped":
                        progress_q.put({"type": "skipped", "source_id": src["id"],
                                        "message": f"  – {src['name']}: skipped (unchanged)"})
                    else:
                        progress_q.put({"type": "warn", "source_id": src["id"],
                                        "message": f"  ✗ {src['name']}: {result.get('message', 'error')}"})
                except Exception as exc:
                    progress_q.put({"type": "warn", "source_id": src["id"],
                                    "message": f"  ✗ {src['name']}: {exc}"})

            progress_q.put({"type": "complete",
                             "message": f"Ingestion complete. {total} sources processed."})
        except Exception as exc:
            progress_q.put({"type": "error", "message": str(exc)})

    threading.Thread(target=ingest_worker, daemon=True).start()

    def generate():
        while True:
            try:
                event = progress_q.get(timeout=120)
                yield f"data: {json.dumps(event)}\n\n"
                if event.get("type") in ("complete", "error"):
                    break
            except queue.Empty:
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"

    return Response(
        stream_with_context(generate()),
        content_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/compliance/chat", methods=["POST"])
def api_compliance_chat():
    """
    POST /api/compliance/chat
    Body: {query, chat_history?:[], api_key?}
    SSE stream.
    """
    data         = request.get_json(force=True, silent=True) or {}
    query        = data.get("query", "").strip()
    chat_history = data.get("chat_history", [])
    api_key      = data.get("api_key") or _ANTHROPIC_API_KEY

    if not query:
        return jsonify({"error": "query is required"}), 400
    if not api_key:
        return jsonify({"error": "Anthropic API key required"}), 400

    try:
        collection = _get_compliance_collection()
        from compliance.chatbot import stream_chat

        def generate():
            for event in stream_chat(query, collection, api_key, chat_history):
                yield event

        return Response(
            stream_with_context(generate()),
            content_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 503
    except Exception as exc:
        logger.exception("Compliance chat failed")
        return jsonify({"error": str(exc)}), 500


@app.route("/api/compliance/explain", methods=["POST"])
def api_compliance_explain():
    """
    POST /api/compliance/explain
    Body: {section_type, context?:"", api_key?}
    Returns: {rationale, citations, standards}
    """
    data         = request.get_json(force=True, silent=True) or {}
    section_type = data.get("section_type", "")
    context      = data.get("context", "")
    api_key      = data.get("api_key") or _ANTHROPIC_API_KEY

    if not section_type:
        return jsonify({"error": "section_type is required"}), 400
    if not api_key:
        return jsonify({"error": "Anthropic API key required"}), 400

    try:
        collection = _get_compliance_collection()
        from compliance.citation_engine import explain_disclosure
        result = explain_disclosure(section_type, context, collection, api_key)
        return jsonify(result)
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 503
    except Exception as exc:
        logger.exception("Compliance explain failed")
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# EDGAR HTML fetch + preview routes
# ---------------------------------------------------------------------------

@app.route("/api/fetch-filing-stream", methods=["POST"])
def fetch_filing_stream():
    """SSE: fetch + transform EDGAR HTML filing. Events: progress | done | error."""
    data             = request.get_json(force=True, silent=True) or {}
    cik              = data.get("cik", DJCO_CIK).strip()
    accession_number = data.get("accession_number", "").strip()
    primary_document = data.get("primary_document", "").strip()
    source_config    = data.get("source_config") or {}
    target_config    = data.get("target_config") or {}
    user_agent       = data.get("user_agent", DEFAULT_USER_AGENT).strip()

    if not accession_number or not primary_document:
        return jsonify({"error": "accession_number and primary_document required"}), 400

    progress_q = queue.Queue()
    html_id    = str(uuid.uuid4())

    def worker():
        try:
            from rollforward.text_updater import build_rules
            from rollforward.html_processor import process_filing_html

            rules = build_rules(source_config, target_config)

            # Fetch EDGAR facts for Phase 2 table blanking + comparable fill
            edgar_lookup = None
            try:
                progress_q.put({"type": "progress", "message": "Fetching EDGAR facts for table roll-forward…"})
                facts_json   = fetch_company_facts(cik, user_agent)
                edgar_lookup = build_fact_lookup(facts_json)
                progress_q.put({"type": "progress",
                                 "message": f"EDGAR: {len(edgar_lookup):,} facts loaded"})
            except Exception as exc:
                progress_q.put({"type": "progress",
                                 "message": f"Warning: EDGAR fetch skipped ({exc}); text-only mode"})

            result = process_filing_html(
                cik=cik, accession_number=accession_number,
                primary_document=primary_document, rules=rules,
                user_agent=user_agent,
                progress_callback=lambda m: progress_q.put({"type": "progress", "message": m}),
                edgar_lookup=edgar_lookup,
                target_config=target_config or {},
            )
            if result["status"] != "ok":
                progress_q.put({"type": "error", "message": result["error"]}); return
            _html_sessions[html_id] = result
            progress_q.put({
                "type":             "done",
                "html_id":          html_id,
                "url":              result["url"],
                "replacements":     result["replacements"],
                "tables_processed": result.get("tables_processed", 0),
                "cells_blanked":    result.get("cells_blanked", 0),
                "edgar_inserted":   result.get("edgar_inserted", 0),
                "edgar_missing":    result.get("edgar_missing", []),
            })
        except Exception as exc:
            logger.exception("fetch-filing-stream failed")
            progress_q.put({"type": "error", "message": str(exc)})

    threading.Thread(target=worker, daemon=True).start()

    def generate():
        while True:
            try:
                event = progress_q.get(timeout=90)
                yield f"data: {json.dumps(event)}\n\n"
                if event.get("type") in ("done", "error"): break
            except queue.Empty:
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"

    return Response(stream_with_context(generate()),
                    content_type="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/load-filing-stream", methods=["POST"])
def load_filing_stream():
    """SSE: fetch raw EDGAR HTML (no substitution) for filing viewer."""
    data             = request.get_json(force=True, silent=True) or {}
    cik              = data.get("cik", "").strip()
    accession_number = data.get("accession_number", "").strip()
    primary_document = data.get("primary_document", "").strip()
    user_agent       = data.get("user_agent", DEFAULT_USER_AGENT).strip()

    if not accession_number or not primary_document:
        return jsonify({"error": "accession_number and primary_document required"}), 400

    progress_q = queue.Queue()
    html_id    = str(uuid.uuid4())

    def worker():
        try:
            from rollforward.html_processor import fetch_filing_html
            url      = build_filing_url(cik, accession_number, primary_document)
            raw_html = fetch_filing_html(url, user_agent)
            _html_sessions[html_id] = {
                "html": raw_html, "url": url,
                "replacements": 0, "tables_processed": 0,
                "cells_blanked": 0, "edgar_inserted": 0, "edgar_missing": [],
            }
            progress_q.put({"type": "done", "html_id": html_id, "url": url})
        except Exception as exc:
            logger.exception("load-filing-stream failed")
            progress_q.put({"type": "error", "message": str(exc)})

    threading.Thread(target=worker, daemon=True).start()

    def generate():
        while True:
            try:
                event = progress_q.get(timeout=90)
                yield f"data: {json.dumps(event)}\n\n"
                if event.get("type") in ("done", "error"):
                    break
            except queue.Empty:
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"

    return Response(stream_with_context(generate()),
                    content_type="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/filing-html/<html_id>")
def serve_filing_html(html_id):
    session = _html_sessions.get(html_id)
    if not session:
        return "HTML session not found", 404
    return Response(session["html"], content_type="text/html; charset=utf-8",
                    headers={"X-Frame-Options": "SAMEORIGIN"})


@app.route("/api/download-filing-html/<html_id>")
def download_filing_html(html_id):
    import io
    session = _html_sessions.get(html_id)
    if not session:
        return jsonify({"error": "Not found"}), 404
    return send_file(io.BytesIO(session["html"].encode("utf-8")),
                     as_attachment=True,
                     download_name=f"Form_10-Q_rolled_{html_id[:8]}.htm",
                     mimetype="text/html")


@app.route("/api/download-filing-html-pdf/<html_id>")
def download_filing_html_pdf(html_id):
    """Convert the rolled-forward EDGAR HTML to PDF via weasyprint."""
    import io
    session = _html_sessions.get(html_id)
    if not session:
        return jsonify({"error": "Not found"}), 404
    try:
        from weasyprint import HTML as WeasyprintHTML
        pdf_bytes = WeasyprintHTML(
            string=session["html"],
            base_url="https://www.sec.gov/",
        ).write_pdf()
    except ImportError:
        return jsonify({"error": "weasyprint is not installed. Run: pip install weasyprint"}), 503
    except Exception as exc:
        logger.exception("PDF generation failed")
        return jsonify({"error": f"PDF generation failed: {exc}"}), 500
    return send_file(
        io.BytesIO(pdf_bytes),
        as_attachment=True,
        download_name=f"Form_10-Q_rolled_{html_id[:8]}.pdf",
        mimetype="application/pdf",
    )


@app.route("/api/rollforward-html-stream", methods=["POST"])
def rollforward_html_stream():
    """SSE: roll-forward an already-fetched EDGAR HTML session (no .docx needed)."""
    data          = request.get_json(force=True, silent=True) or {}
    html_id       = data.get("html_id", "").strip()
    cik           = data.get("cik", DJCO_CIK).strip()
    source_config = data.get("source_config") or {}
    target_config = data.get("target_config") or {}
    user_agent    = data.get("user_agent", DEFAULT_USER_AGENT).strip()

    session = _html_sessions.get(html_id)
    if not session:
        return jsonify({"error": "html_id not found or expired — reload the filing first"}), 400

    progress_q  = queue.Queue()
    new_html_id = str(uuid.uuid4())

    def worker():
        try:
            from rollforward.text_updater import build_rules
            from rollforward.html_processor import transform_html_text
            from bs4 import BeautifulSoup

            raw_html = session["html"]
            rules    = build_rules(source_config, target_config)

            progress_q.put({"type": "progress",
                             "message": f"Applying {len(rules)} text substitution rules…"})
            modified_html, count = transform_html_text(raw_html, rules)
            progress_q.put({"type": "progress",
                             "message": f"Text pass complete — {count} replacements"})

            table_stats = {
                "tables_processed": 0, "cells_blanked": 0,
                "edgar_inserted": 0, "edgar_missing": [],
            }

            if cik and target_config:
                edgar_lookup = None
                try:
                    progress_q.put({"type": "progress",
                                     "message": "Fetching EDGAR facts for table roll-forward…"})
                    facts_json   = fetch_company_facts(cik, user_agent)
                    edgar_lookup = build_fact_lookup(facts_json)
                    progress_q.put({"type": "progress",
                                     "message": f"EDGAR: {len(edgar_lookup):,} facts loaded"})
                except Exception as exc:
                    progress_q.put({"type": "progress",
                                     "message": f"Warning: EDGAR fetch skipped ({exc}); text-only mode"})

                if edgar_lookup:
                    try:
                        from rollforward.html_table_processor import process_html_tables
                        progress_q.put({"type": "progress",
                                         "message": "Applying table roll-forward (blanking + EDGAR fill)…"})
                        soup = BeautifulSoup(modified_html, "lxml")
                        table_stats = process_html_tables(soup, target_config, edgar_lookup)
                        modified_html = str(soup)
                        progress_q.put({"type": "progress",
                                         "message": (
                                             f"Table pass complete — "
                                             f"{table_stats['tables_processed']} tables, "
                                             f"{table_stats['cells_blanked']} cells blanked, "
                                             f"{table_stats['edgar_inserted']} EDGAR values inserted"
                                         )})
                    except Exception as exc:
                        logger.exception("HTML table pass failed")
                        progress_q.put({"type": "progress",
                                         "message": f"Warning: table pass failed ({exc})"})

            _html_sessions[new_html_id] = {
                "html":       modified_html,
                "url":        session.get("url", ""),
                "replacements": count,
                **table_stats,
            }
            progress_q.put({
                "type":             "done",
                "html_id":          new_html_id,
                "url":              session.get("url", ""),
                "replacements":     count,
                "tables_processed": table_stats["tables_processed"],
                "cells_blanked":    table_stats["cells_blanked"],
                "edgar_inserted":   table_stats["edgar_inserted"],
                "edgar_missing":    table_stats["edgar_missing"],
            })
        except Exception as exc:
            logger.exception("rollforward-html-stream failed")
            progress_q.put({"type": "error", "message": str(exc)})

    threading.Thread(target=worker, daemon=True).start()

    def generate():
        while True:
            try:
                event = progress_q.get(timeout=90)
                yield f"data: {json.dumps(event)}\n\n"
                if event.get("type") in ("done", "error"):
                    break
            except queue.Empty:
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"

    return Response(
        stream_with_context(generate()),
        content_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/save-html-edits/<html_id>", methods=["POST"])
def save_html_edits(html_id):
    """POST /api/save-html-edits/<html_id> — Persist browser-edited HTML back to session."""
    session = _html_sessions.get(html_id)
    if not session:
        return jsonify({"error": "html_id not found or expired"}), 404
    data         = request.get_json(force=True, silent=True) or {}
    html_content = data.get("html", "")
    if not html_content:
        return jsonify({"error": "html field required"}), 400
    session["html"] = html_content
    logger.info("Saved edits to html_id %s (%d bytes)", html_id, len(html_content))
    return jsonify({"saved": True, "html_id": html_id, "size": len(html_content)})


@app.route("/api/raw-upload/<file_id>")
def raw_upload(file_id):
    """Serve the raw .docx bytes so the browser can render it with docx-preview.js."""
    path = _uploaded_files.get(file_id)
    if not path or not os.path.exists(path):
        return "File not found", 404
    return send_file(
        path,
        mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )


@app.route("/api/preview-upload-html/<file_id>")
def preview_upload_html(file_id):
    """GET /api/preview-upload-html/<file_id> — Serve uploaded .docx as inline HTML via mammoth."""
    path = _uploaded_files.get(file_id)
    if not path or not os.path.exists(path):
        return "File not found", 404
    try:
        import mammoth
        with open(path, "rb") as f:
            result = mammoth.convert_to_html(f)
        html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<style>body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
font-size:13px;line-height:1.6;padding:2rem 3rem;max-width:900px;margin:0 auto;color:#111}}
table{{border-collapse:collapse;width:100%}}td,th{{border:1px solid #d1d5db;padding:4px 8px;font-size:12px}}
</style></head><body>{result.value}</body></html>"""
        return Response(html, content_type="text/html; charset=utf-8")
    except ImportError:
        return "mammoth not installed — run: pip install mammoth", 503
    except Exception as exc:
        logger.exception("preview-upload-html failed")
        return f"Preview failed: {exc}", 500


@app.route("/api/download-job-html/<job_id>")
def download_job_html(job_id):
    """Convert a rolled-forward .docx to HTML via mammoth."""
    import io
    job = _jobs.get(job_id)
    if not job or not os.path.exists(job["output_path"]):
        return jsonify({"error": "Job not found or expired"}), 404
    try:
        import mammoth
        with open(job["output_path"], "rb") as f:
            result = mammoth.convert_to_html(f)
        html_bytes = result.value.encode("utf-8")
    except ImportError:
        return jsonify({"error": "mammoth is not installed. Run: pip install mammoth"}), 503
    except Exception as exc:
        logger.exception("HTML export failed")
        return jsonify({"error": f"HTML export failed: {exc}"}), 500
    fname = job.get("output_filename", "Form_10-Q_draft.docx").replace(".docx", ".html")
    return send_file(io.BytesIO(html_bytes), as_attachment=True,
                     download_name=fname, mimetype="text/html")


@app.route("/api/download-job-pdf/<job_id>")
def download_job_pdf(job_id):
    """Convert a rolled-forward .docx to PDF via mammoth → weasyprint."""
    import io
    job = _jobs.get(job_id)
    if not job or not os.path.exists(job["output_path"]):
        return jsonify({"error": "Job not found or expired"}), 404
    try:
        import mammoth
        from weasyprint import HTML as WeasyprintHTML
        with open(job["output_path"], "rb") as f:
            html_str = mammoth.convert_to_html(f).value
        pdf_bytes = WeasyprintHTML(string=html_str).write_pdf()
    except ImportError as exc:
        return jsonify({"error": f"Missing library: {exc}. Run: pip install mammoth weasyprint"}), 503
    except Exception as exc:
        logger.exception("PDF export failed")
        return jsonify({"error": f"PDF export failed: {exc}"}), 500
    fname = job.get("output_filename", "Form_10-Q_draft.docx").replace(".docx", ".pdf")
    return send_file(io.BytesIO(pdf_bytes), as_attachment=True,
                     download_name=fname, mimetype="application/pdf")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True, port=5000)
