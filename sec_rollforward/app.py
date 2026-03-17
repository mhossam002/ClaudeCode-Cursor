"""
app.py — Flask web application for SEC 10-Q Roll-Forward Tool.
"""
import os
import uuid
import logging
import tempfile
import json

from flask import Flask, render_template, request, jsonify, send_file, abort

from rollforward.docx_parser import load_document, extract_table_map
from rollforward.edgar_client import fetch_company_facts, build_fact_lookup, detect_period_config
from rollforward.engine import roll_forward, DJCO_CIK, Q1_CONFIG, Q2_CONFIG

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB upload limit

# Temp storage: {file_id: abs_path}
_uploaded_files: dict = {}

UPLOAD_DIR = os.path.join(tempfile.gettempdir(), "sec_rollforward_uploads")
OUTPUT_DIR = os.path.join(tempfile.gettempdir(), "sec_rollforward_outputs")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    """
    POST /upload
    Accepts a .docx file, saves it to temp dir, returns:
        {file_id, filename, table_map}
    """
    if "file" not in request.files:
        return jsonify({"error": "No file field in request"}), 400

    f = request.files["file"]
    if not f.filename.endswith(".docx"):
        return jsonify({"error": "Only .docx files are accepted"}), 400

    file_id = str(uuid.uuid4())
    dest_path = os.path.join(UPLOAD_DIR, f"{file_id}.docx")
    f.save(dest_path)
    _uploaded_files[file_id] = dest_path

    try:
        doc = load_document(dest_path)
        table_map = extract_table_map(doc)
    except Exception as exc:
        logger.exception("Failed to parse uploaded document")
        return jsonify({"error": f"Failed to parse document: {exc}"}), 500

    return jsonify({
        "file_id":   file_id,
        "filename":  f.filename,
        "table_map": table_map,
    })


@app.route("/api/detect-period")
def api_detect_period():
    """
    GET /api/detect-period?cik=...&user_agent=...
    Fetches EDGAR submissions for the CIK and returns auto-detected
    source + target period configs derived from the latest 10-Q filing.
    """
    cik        = request.args.get("cik", DJCO_CIK).strip()
    user_agent = request.args.get("user_agent", "").strip()
    if not user_agent:
        return jsonify({"error": "user_agent is required"}), 400
    try:
        result = detect_period_config(cik, user_agent)
    except Exception as exc:
        logger.exception("Period detection failed")
        return jsonify({"error": str(exc)}), 502
    return jsonify(result)


@app.route("/rollforward", methods=["POST"])
def do_rollforward():
    """
    POST /rollforward
    Body (JSON):
        file_id      : from /upload
        cik          : SEC CIK
        user_agent   : SEC User-Agent string
        add_ytd      : bool (default false)
        source_config: period config dict (optional — defaults to Q1_CONFIG)
        target_config: period config dict (optional — defaults to Q2_CONFIG)
    Returns the output .docx as a file download.
    """
    data = request.get_json(force=True, silent=True) or {}
    file_id       = data.get("file_id")
    cik           = data.get("cik", DJCO_CIK).strip()
    user_agent    = data.get("user_agent", "DJCO SEC Tool admin@example.com").strip()
    add_ytd       = bool(data.get("add_ytd", False))
    source_config = data.get("source_config") or None
    target_config = data.get("target_config") or None

    if not file_id or file_id not in _uploaded_files:
        return jsonify({"error": "Invalid or expired file_id. Please re-upload."}), 400

    source_path = _uploaded_files[file_id]

    # Build a descriptive output filename from the target period
    if target_config and target_config.get("period_label"):
        safe_label = (target_config["period_label"]
                      .replace(" ", "_").replace(",", "").replace("/", "-"))
        output_filename = f"Form_10-Q_{safe_label}_draft_{file_id[:8]}.docx"
    else:
        output_filename = f"Form_10-Q_draft_{file_id[:8]}.docx"
    output_path = os.path.join(OUTPUT_DIR, output_filename)

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

    return send_file(
        output_path,
        as_attachment=True,
        download_name=output_filename,
        mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )


@app.route("/edgar-preview")
def edgar_preview():
    """
    GET /edgar-preview?cik=...&period_end=...&user_agent=...
    Returns JSON of all EDGAR facts found for the given period.
    """
    cik        = request.args.get("cik", DJCO_CIK).strip()
    period_end = request.args.get("period_end", Q2_CONFIG["comparable_end"]).strip()
    user_agent = request.args.get("user_agent", "DJCO SEC Tool admin@example.com").strip()

    try:
        facts_json = fetch_company_facts(cik, user_agent)
        lookup = build_fact_lookup(facts_json)
    except Exception as exc:
        return jsonify({"error": f"EDGAR fetch failed: {exc}"}), 502

    # Filter to entries matching the requested period_end
    preview = {}
    for (concept, end, months), val in lookup.items():
        if end == period_end:
            key = f"{concept}/{months}mo"
            preview[key] = val

    return jsonify({
        "cik":        cik,
        "period_end": period_end,
        "facts":      preview,
        "count":      len(preview),
    })


@app.route("/stats/<file_id>")
def get_stats(file_id):
    """Return cached stats for a completed roll-forward (not persisted — placeholder)."""
    return jsonify({"error": "Stats not persisted between requests. Check roll-forward response headers."}), 404


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True, port=5000)
