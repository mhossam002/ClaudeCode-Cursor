"""
session_store.py — SQLite-backed session management for uploaded files and roll-forward outputs.
Auto-expires sessions after 24 hours and cleans up temp files.
"""

import sqlite3
import os
import time
import threading
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Default DB path in the system temp directory
_DEFAULT_DB_PATH = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMP", "/tmp")),
    "sec_rollforward",
    "sessions.db",
)

# Module-level lock for thread safety
_lock = threading.Lock()


def _get_conn(db_path: str) -> sqlite3.Connection:
    """Return a SQLite connection with row_factory set."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str = _DEFAULT_DB_PATH) -> None:
    """Create the sessions and outputs tables if they do not already exist.

    Args:
        db_path: Filesystem path to the SQLite database file.
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with _lock:
        conn = _get_conn(db_path)
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id   TEXT PRIMARY KEY,
                    file_path    TEXT,
                    filename     TEXT,
                    created_at   REAL,
                    last_accessed REAL,
                    status       TEXT
                );

                CREATE TABLE IF NOT EXISTS outputs (
                    output_id       TEXT PRIMARY KEY,
                    session_id      TEXT,
                    output_path     TEXT,
                    output_filename TEXT,
                    created_at      REAL
                );
                """
            )
            conn.commit()
            logger.debug("session_store: DB initialised at %s", db_path)
        finally:
            conn.close()


def add_session(
    session_id: str,
    file_path: str,
    filename: str,
    db_path: str = _DEFAULT_DB_PATH,
) -> None:
    """Insert a new session record.

    Args:
        session_id: UUID string for the session.
        file_path:  Absolute path to the uploaded temp file.
        filename:   Original filename as supplied by the user.
        db_path:    Path to the SQLite database.
    """
    now = time.time()
    with _lock:
        conn = _get_conn(db_path)
        try:
            conn.execute(
                "INSERT INTO sessions (session_id, file_path, filename, created_at, last_accessed, status) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (session_id, file_path, filename, now, now, "ready"),
            )
            conn.commit()
            logger.debug("session_store: added session %s (%s)", session_id, filename)
        finally:
            conn.close()


def get_session(
    session_id: str, db_path: str = _DEFAULT_DB_PATH
) -> Optional[dict]:
    """Fetch a session by ID.

    Args:
        session_id: UUID string to look up.
        db_path:    Path to the SQLite database.

    Returns:
        A dict with session fields, or None if not found.
    """
    with _lock:
        conn = _get_conn(db_path)
        try:
            row = conn.execute(
                "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()


def touch_session(session_id: str, db_path: str = _DEFAULT_DB_PATH) -> None:
    """Update last_accessed timestamp for the given session.

    Args:
        session_id: UUID string of the session to touch.
        db_path:    Path to the SQLite database.
    """
    with _lock:
        conn = _get_conn(db_path)
        try:
            conn.execute(
                "UPDATE sessions SET last_accessed = ? WHERE session_id = ?",
                (time.time(), session_id),
            )
            conn.commit()
        finally:
            conn.close()


def add_output(
    output_id: str,
    session_id: str,
    output_path: str,
    output_filename: str,
    db_path: str = _DEFAULT_DB_PATH,
) -> None:
    """Store a roll-forward output record.

    Args:
        output_id:       UUID string for this output.
        session_id:      Parent session UUID.
        output_path:     Absolute path to the generated file.
        output_filename: Filename to present to the user.
        db_path:         Path to the SQLite database.
    """
    now = time.time()
    with _lock:
        conn = _get_conn(db_path)
        try:
            conn.execute(
                "INSERT INTO outputs (output_id, session_id, output_path, output_filename, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (output_id, session_id, output_path, output_filename, now),
            )
            conn.commit()
            logger.debug(
                "session_store: added output %s for session %s", output_id, session_id
            )
        finally:
            conn.close()


def list_sessions(db_path: str = _DEFAULT_DB_PATH) -> list:
    """Return all sessions as a list of dicts.

    Args:
        db_path: Path to the SQLite database.

    Returns:
        List of session dicts ordered by created_at descending.
    """
    with _lock:
        conn = _get_conn(db_path)
        try:
            rows = conn.execute(
                "SELECT * FROM sessions ORDER BY created_at DESC"
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def delete_session(session_id: str, db_path: str = _DEFAULT_DB_PATH) -> None:
    """Delete a session and all its outputs from the database.

    Does NOT delete files from disk — call cleanup_expired for that.

    Args:
        session_id: UUID string of the session to delete.
        db_path:    Path to the SQLite database.
    """
    with _lock:
        conn = _get_conn(db_path)
        try:
            conn.execute("DELETE FROM outputs WHERE session_id = ?", (session_id,))
            conn.execute("DELETE FROM sessions WHERE session_id = ?", (session_id,))
            conn.commit()
            logger.debug("session_store: deleted session %s", session_id)
        finally:
            conn.close()


def cleanup_expired(
    max_age_seconds: float = 86400, db_path: str = _DEFAULT_DB_PATH
) -> list:
    """Delete expired sessions (and their outputs) from the DB and disk.

    A session is considered expired when
    ``time.time() - last_accessed > max_age_seconds``.

    Args:
        max_age_seconds: Maximum age in seconds before a session expires.
                         Defaults to 86 400 (24 hours).
        db_path:         Path to the SQLite database.

    Returns:
        List of session_id strings that were deleted.
    """
    cutoff = time.time() - max_age_seconds
    deleted_ids: list = []

    with _lock:
        conn = _get_conn(db_path)
        try:
            expired = conn.execute(
                "SELECT session_id, file_path FROM sessions WHERE last_accessed < ?",
                (cutoff,),
            ).fetchall()

            for row in expired:
                sid = row["session_id"]
                fpath = row["file_path"]

                # Remove temp file if it exists
                if fpath and os.path.exists(fpath):
                    try:
                        os.remove(fpath)
                        logger.debug(
                            "session_store: removed temp file %s for session %s",
                            fpath,
                            sid,
                        )
                    except OSError as exc:
                        logger.warning(
                            "session_store: could not remove %s: %s", fpath, exc
                        )

                # Remove output files
                outputs = conn.execute(
                    "SELECT output_path FROM outputs WHERE session_id = ?", (sid,)
                ).fetchall()
                for out_row in outputs:
                    opath = out_row["output_path"]
                    if opath and os.path.exists(opath):
                        try:
                            os.remove(opath)
                        except OSError as exc:
                            logger.warning(
                                "session_store: could not remove output %s: %s",
                                opath,
                                exc,
                            )

                # Delete from DB
                conn.execute("DELETE FROM outputs WHERE session_id = ?", (sid,))
                conn.execute("DELETE FROM sessions WHERE session_id = ?", (sid,))
                deleted_ids.append(sid)

            conn.commit()
            if deleted_ids:
                logger.info(
                    "session_store: expired and cleaned up %d sessions", len(deleted_ids)
                )
        finally:
            conn.close()

    return deleted_ids


def get_session_count(db_path: str = _DEFAULT_DB_PATH) -> int:
    """Return the total number of sessions currently stored.

    Args:
        db_path: Path to the SQLite database.

    Returns:
        Integer count of rows in the sessions table.
    """
    with _lock:
        conn = _get_conn(db_path)
        try:
            row = conn.execute("SELECT COUNT(*) AS cnt FROM sessions").fetchone()
            return row["cnt"] if row else 0
        finally:
            conn.close()
