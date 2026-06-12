#!/usr/bin/env python3
"""
ups_exception_watcher.py
========================

Watches a Gmail inbox for UPS *exception* emails, extracts the tracking
numbers, looks each one up via the UPS Track REST API, keeps an open/closed
list in a local SQLite database, and emails you a summary ONLY when a tracked
shipment's status changes (including when it finally delivers / resolves).

Designed to be run on a schedule by cron / launchd / Windows Task Scheduler.
Each run does four things:

    1. Reads new unseen UPS exception emails (IMAP) and pulls out 1Z numbers.
    2. Adds any new tracking numbers to the watch list.
    3. Re-checks every open (unresolved) shipment against the UPS Track API.
    4. Emails a change-only summary and marks delivered shipments resolved.

NOTHING SENSITIVE LIVES IN THIS FILE. All secrets come from environment
variables (see "Required environment" below).

Dependencies:  requests   (everything else is the Python standard library)
    pip install requests

------------------------------------------------------------------------------
Required environment
------------------------------------------------------------------------------
    GMAIL_USER            the dedicated Gmail address that receives forwards
    GMAIL_APP_PASSWORD    a Gmail App Password (needs 2-Step Verification on)
    UPS_CLIENT_ID         from your UPS Developer Portal app
    UPS_CLIENT_SECRET     from your UPS Developer Portal app

Optional environment (sensible defaults shown)
    UPS_ENVIRONMENT       "production" (default) or "test"
    NOTIFY_TO             where alerts are sent  (default: GMAIL_USER)
    IMAP_FOLDER           Gmail label/folder to read (default: "INBOX")
    IMAP_HOST             default "imap.gmail.com"
    SMTP_HOST             default "smtp.gmail.com"
    SMTP_PORT             default 465
    FROM_FILTER           sender substring to match (default: "ups.com")
    DB_PATH               default "ups_watch.db" (in the working directory)
    MARK_SEEN             "true" (default) marks processed emails as read

------------------------------------------------------------------------------
Example schedule (run every 15 minutes)
------------------------------------------------------------------------------
    crontab -e
    */15 * * * * cd /path/to/dir && /usr/bin/python3 ups_exception_watcher.py >> watcher.log 2>&1

NOTE: This watcher monitors status and alerts on change. It intentionally does
NOT compute on-time vs. late (service-level cutoffs, business-day math). That
logic already exists in your ups-late-delivery skill and can be layered on
later if you want late-flagging here too.
"""

import os
import re
import sys
import time
import uuid
import html
import sqlite3
import logging
from datetime import datetime, timezone

import requests

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

CONFIG = {
    "gmail_user":         os.environ.get("GMAIL_USER"),
    "gmail_app_password": os.environ.get("GMAIL_APP_PASSWORD"),
    "imap_host":          os.environ.get("IMAP_HOST", "imap.gmail.com"),
    "imap_folder":        os.environ.get("IMAP_FOLDER", "INBOX"),
    "smtp_host":          os.environ.get("SMTP_HOST", "smtp.gmail.com"),
    "smtp_port":          int(os.environ.get("SMTP_PORT", "465")),
    "notify_to":          os.environ.get("NOTIFY_TO"),
    "from_filter":        os.environ.get("FROM_FILTER", "ups.com"),
    "ups_client_id":      os.environ.get("UPS_CLIENT_ID"),
    "ups_client_secret":  os.environ.get("UPS_CLIENT_SECRET"),
    "ups_environment":    os.environ.get("UPS_ENVIRONMENT", "production").lower(),
    "db_path":            os.environ.get("DB_PATH", "ups_watch.db"),
    "mark_seen":          os.environ.get("MARK_SEEN", "true").lower() == "true",
}

UPS_BASE = {
    "test":       "https://wwwcie.ups.com",
    "production": "https://onlinetools.ups.com",
}

# Only treat a UPS email as an exception if the subject/body mentions one of
# these. Tune to match what your account actually receives.
EXCEPTION_KEYWORDS = [
    "exception", "delivery attempt", "address correction", "delayed",
    "action required", "unable to deliver", "rescheduled", "held",
    "we need", "could not be delivered",
]

# UPS 1Z tracking number: 1Z + 16 alphanumeric characters.
TRACKING_RE = re.compile(r"\b1Z[0-9A-Z]{16}\b", re.IGNORECASE)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ups-watcher").info


def validate_config():
    required = ["gmail_user", "gmail_app_password", "ups_client_id", "ups_client_secret"]
    missing = [k.upper() for k in required if not CONFIG[k]]
    if missing:
        sys.exit(f"Missing required environment variable(s): {', '.join(missing)}")
    if CONFIG["ups_environment"] not in UPS_BASE:
        sys.exit("UPS_ENVIRONMENT must be 'test' or 'production'.")
    if not CONFIG["notify_to"]:
        CONFIG["notify_to"] = CONFIG["gmail_user"]


# --------------------------------------------------------------------------- #
# Database
# --------------------------------------------------------------------------- #

def init_db():
    conn = sqlite3.connect(CONFIG["db_path"])
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS shipments (
            tracking_number    TEXT PRIMARY KEY,
            first_seen         TEXT,
            last_checked       TEXT,
            last_status        TEXT,
            last_status_code   TEXT,
            last_scan_location TEXT,
            last_scan_time     TEXT,
            scheduled_delivery TEXT,
            resolved           INTEGER DEFAULT 0,
            source_subject     TEXT
        )
        """
    )
    conn.commit()
    return conn


def add_tracking(conn, tn, subject):
    """Insert a newly seen tracking number. Returns True if it was new."""
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    cur = conn.execute(
        "INSERT OR IGNORE INTO shipments (tracking_number, first_seen, source_subject) "
        "VALUES (?, ?, ?)",
        (tn, now, subject),
    )
    conn.commit()
    return cur.rowcount > 0


def get_open_shipments(conn):
    return conn.execute(
        "SELECT * FROM shipments WHERE resolved = 0 ORDER BY first_seen"
    ).fetchall()


def update_shipment(conn, tn, info):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    conn.execute(
        """
        UPDATE shipments SET
            last_checked       = ?,
            last_status        = ?,
            last_status_code   = ?,
            last_scan_location = ?,
            last_scan_time     = ?,
            scheduled_delivery = ?,
            resolved           = ?
        WHERE tracking_number = ?
        """,
        (
            now,
            info["status"],
            info["status_code"],
            info["last_scan_location"],
            info["last_scan_time"],
            info["scheduled_delivery"],
            1 if info["delivered"] else 0,
            tn,
        ),
    )
    conn.commit()


# --------------------------------------------------------------------------- #
# Gmail (IMAP) ingestion
# --------------------------------------------------------------------------- #

def _strip_html(raw):
    raw = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", raw, flags=re.S | re.I)
    raw = re.sub(r"<[^>]+>", " ", raw)
    return html.unescape(re.sub(r"\s+", " ", raw))


def _message_text(msg):
    """Return subject + best-effort body text for keyword/regex scanning."""
    import email
    from email.header import decode_header

    def decode(value):
        if not value:
            return ""
        parts = decode_header(value)
        out = ""
        for text, enc in parts:
            if isinstance(text, bytes):
                out += text.decode(enc or "utf-8", errors="replace")
            else:
                out += text
        return out

    subject = decode(msg.get("Subject"))
    body = ""
    if msg.is_multipart():
        plain, htmltext = "", ""
        for part in msg.walk():
            ctype = part.get_content_type()
            if part.get_content_disposition() == "attachment":
                continue
            try:
                payload = part.get_payload(decode=True)
            except Exception:
                continue
            if not payload:
                continue
            charset = part.get_content_charset() or "utf-8"
            decoded = payload.decode(charset, errors="replace")
            if ctype == "text/plain":
                plain += decoded
            elif ctype == "text/html":
                htmltext += decoded
        body = plain if plain.strip() else _strip_html(htmltext)
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            decoded = payload.decode(charset, errors="replace")
            body = decoded if msg.get_content_type() == "text/plain" else _strip_html(decoded)

    return subject, body


def fetch_new_exceptions():
    """Return a list of (tracking_number, subject) from new UPS exception mail."""
    import imaplib
    import email as email_mod

    found = []
    M = imaplib.IMAP4_SSL(CONFIG["imap_host"])
    try:
        M.login(CONFIG["gmail_user"], CONFIG["gmail_app_password"])
        M.select(CONFIG["imap_folder"])
        criteria = f'(UNSEEN FROM "{CONFIG["from_filter"]}")'
        typ, data = M.search(None, criteria)
        if typ != "OK" or not data or not data[0]:
            return found

        for num in data[0].split():
            # BODY.PEEK avoids auto-marking the message as seen on fetch.
            typ, msg_data = M.fetch(num, "(BODY.PEEK[])")
            if typ != "OK":
                continue
            msg = email_mod.message_from_bytes(msg_data[0][1])
            subject, body = _message_text(msg)
            haystack = f"{subject}\n{body}".lower()

            if not any(kw in haystack for kw in EXCEPTION_KEYWORDS):
                continue  # a UPS email, but not an exception we care about

            numbers = {m.group(0).upper() for m in TRACKING_RE.finditer(f"{subject} {body}")}
            for tn in numbers:
                found.append((tn, subject[:200]))

            if CONFIG["mark_seen"]:
                M.store(num, "+FLAGS", "\\Seen")
    finally:
        try:
            M.close()
        except Exception:
            pass
        M.logout()

    return found


# --------------------------------------------------------------------------- #
# UPS Track REST API
# --------------------------------------------------------------------------- #

def get_ups_token():
    base = UPS_BASE[CONFIG["ups_environment"]]
    resp = requests.post(
        f"{base}/security/v1/oauth/token",
        auth=(CONFIG["ups_client_id"], CONFIG["ups_client_secret"]),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "client_credentials"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _fmt_date(d):
    return f"{d[4:6]}/{d[6:8]}/{d[0:4]}" if d and len(d) == 8 else (d or None)


def _fmt_datetime(d, t):
    ds = _fmt_date(d) or ""
    if t and len(t) >= 4:
        return f"{ds} {t[0:2]}:{t[2:4]}".strip()
    return ds or None


def parse_track_response(payload):
    out = {
        "status": None, "status_type": None, "status_code": None,
        "last_scan_location": None, "last_scan_time": None,
        "scheduled_delivery": None, "delivered": False,
    }
    try:
        pkg = payload["trackResponse"]["shipment"][0]["package"][0]
    except (KeyError, IndexError, TypeError):
        return out

    activities = pkg.get("activity") or []
    if activities:
        latest = activities[0]  # UPS returns newest activity first
        st = latest.get("status", {}) or {}
        out["status"] = st.get("description")
        out["status_type"] = st.get("type")
        out["status_code"] = st.get("statusCode") or st.get("code")
        out["last_scan_time"] = _fmt_datetime(latest.get("date"), latest.get("time"))
        loc = (latest.get("location", {}) or {}).get("address", {}) or {}
        parts = [loc.get("city"), loc.get("stateProvince"), loc.get("country")]
        out["last_scan_location"] = ", ".join(p for p in parts if p) or None

    desc = (out["status"] or "").lower()
    out["delivered"] = out["status_type"] == "D" or "delivered" in desc

    for entry in (pkg.get("deliveryDate") or []):
        if entry.get("type") in ("SDD", "RDD", "EDW"):
            out["scheduled_delivery"] = _fmt_date(entry.get("date"))
            break

    return out


def track_package(token, tn):
    base = UPS_BASE[CONFIG["ups_environment"]]
    resp = requests.get(
        f"{base}/api/track/v1/details/{tn}",
        headers={
            "Authorization": f"Bearer {token}",
            "transId": str(uuid.uuid4()),
            "transactionSrc": "exception-watcher",
            "Content-Type": "application/json",
        },
        params={"locale": "en_US", "returnMilestones": "true", "returnPOD": "false"},
        timeout=30,
    )
    resp.raise_for_status()
    return parse_track_response(resp.json())


# --------------------------------------------------------------------------- #
# Email summary out (SMTP)
# --------------------------------------------------------------------------- #

def build_html(changes):
    rows = ""
    for c in changes:
        resolved_badge = " &#9989; resolved" if c["resolved"] else ""
        rows += (
            "<tr>"
            f"<td style='padding:6px 10px;font-family:monospace'>{c['tracking_number']}</td>"
            f"<td style='padding:6px 10px'><b>{html.escape(c['status'] or 'Unknown')}</b>{resolved_badge}</td>"
            f"<td style='padding:6px 10px'>{html.escape(c['previous'] or '—')}</td>"
            f"<td style='padding:6px 10px'>{html.escape(c['last_scan_location'] or '—')}</td>"
            f"<td style='padding:6px 10px'>{html.escape(c['last_scan_time'] or '—')}</td>"
            f"<td style='padding:6px 10px'>{html.escape(c['scheduled_delivery'] or '—')}</td>"
            "</tr>"
        )
    return f"""
    <div style="font-family:Arial,Helvetica,sans-serif;color:#222">
      <h2 style="margin:0 0 4px">UPS exception update</h2>
      <p style="margin:0 0 12px;color:#666">{len(changes)} status change(s) detected.</p>
      <table style="border-collapse:collapse;font-size:14px">
        <thead>
          <tr style="background:#f3f3f3;text-align:left">
            <th style="padding:6px 10px">Tracking #</th>
            <th style="padding:6px 10px">New status</th>
            <th style="padding:6px 10px">Previous</th>
            <th style="padding:6px 10px">Last scan</th>
            <th style="padding:6px 10px">When</th>
            <th style="padding:6px 10px">Scheduled</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    """


def send_summary(changes):
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"UPS exception update — {len(changes)} change(s)"
    msg["From"] = CONFIG["gmail_user"]
    msg["To"] = CONFIG["notify_to"]
    msg.attach(MIMEText(build_html(changes), "html"))

    with smtplib.SMTP_SSL(CONFIG["smtp_host"], CONFIG["smtp_port"]) as s:
        s.login(CONFIG["gmail_user"], CONFIG["gmail_app_password"])
        s.sendmail(msg["From"], [msg["To"]], msg.as_string())


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main():
    validate_config()
    conn = init_db()

    # 1 + 2: ingest new exception tracking numbers from email
    try:
        new = fetch_new_exceptions()
    except Exception as e:
        log(f"Email ingest failed: {e}")
        new = []
    added = sum(add_tracking(conn, tn, subj) for tn, subj in new)
    log(f"Email: found {len(new)} tracking reference(s), {added} new added to watch list.")

    # 3: re-check open shipments
    open_sh = get_open_shipments(conn)
    if not open_sh:
        log("No open shipments to check. Done.")
        conn.close()
        return

    try:
        token = get_ups_token()
    except Exception as e:
        log(f"Could not obtain UPS token: {e}")
        conn.close()
        return

    changes = []
    for row in open_sh:
        tn = row["tracking_number"]
        try:
            info = track_package(token, tn)
        except Exception as e:
            log(f"  {tn}: lookup failed: {e}")
            continue

        if info["status"] and info["status"] != row["last_status"]:
            changes.append({
                "tracking_number": tn,
                "status": info["status"],
                "previous": row["last_status"],
                "last_scan_location": info["last_scan_location"],
                "last_scan_time": info["last_scan_time"],
                "scheduled_delivery": info["scheduled_delivery"],
                "resolved": info["delivered"],
            })
            log(f"  {tn}: '{row['last_status']}' -> '{info['status']}'"
                f"{' (resolved)' if info['delivered'] else ''}")

        update_shipment(conn, tn, info)
        time.sleep(0.5)  # be polite to the API

    # 4: notify on changes only
    if changes:
        try:
            send_summary(changes)
            log(f"Sent summary email with {len(changes)} change(s) to {CONFIG['notify_to']}.")
        except Exception as e:
            log(f"Failed to send summary email: {e}")
    else:
        log("No status changes this run.")

    conn.close()


if __name__ == "__main__":
    main()
