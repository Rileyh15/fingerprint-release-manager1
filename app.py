"""
Fingerprint Release Manager
Integrates with Accio Data XML API to automate fingerprint release form distribution.

Workflow:
1. Receives applicant data via XML push from Accio Data (or manual entry)
2. Manages a pool of IdentoGO one-time payment codes (imported from Excel)
3. Assigns a code to each applicant
4. Emails the applicant their fingerprint release form PDF with their assigned code

Built with Python standard library + openpyxl for Excel support.
"""

import os
import sys
import json
import sqlite3
import smtplib
import logging
import urllib.parse
import cgi
import io
import csv
import shutil
import base64
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from xml.etree import ElementTree as ET
from string import Template

try:
    import openpyxl
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False
    print("WARNING: openpyxl not installed. Excel import will be limited to CSV only.")
    print("Install with: pip install openpyxl")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "fingerprint.db")
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
WATCH_FOLDER = os.path.join(BASE_DIR, "watch")  # Drop Excel files here for auto-import
PROCESSED_FOLDER = os.path.join(BASE_DIR, "watch", "processed")  # Auto-imported files move here
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")
HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 5000))

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(WATCH_FOLDER, exist_ok=True)
os.makedirs(PROCESSED_FOLDER, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
    return db

def init_db():
    db = get_db()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS applicants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT,
            phone TEXT,
            accio_order_number TEXT,
            accio_remote_number TEXT,
            status TEXT DEFAULT 'pending',
            assigned_code TEXT,
            email_sent INTEGER DEFAULT 0,
            email_sent_at TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now')),
            notes TEXT
        );
        CREATE TABLE IF NOT EXISTS codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL UNIQUE,
            status TEXT DEFAULT 'available',
            assigned_to INTEGER,
            assigned_at TEXT,
            imported_at TEXT DEFAULT (datetime('now')),
            batch_name TEXT,
            FOREIGN KEY (assigned_to) REFERENCES applicants(id)
        );
        CREATE TABLE IF NOT EXISTS email_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            applicant_id INTEGER,
            recipient_email TEXT,
            subject TEXT,
            status TEXT,
            error_message TEXT,
            sent_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (applicant_id) REFERENCES applicants(id)
        );
        CREATE TABLE IF NOT EXISTS xml_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            direction TEXT,
            raw_xml TEXT,
            parsed_status TEXT,
            error_message TEXT,
            received_at TEXT DEFAULT (datetime('now'))
        );
    """)
    db.commit()
    db.close()

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
DEFAULT_SETTINGS = {
    "accio_account": "",
    "accio_username": "",
    "accio_password": "",
    "accio_post_url": "",
    "smtp_server": "",
    "smtp_port": "587",
    "smtp_username": "",
    "smtp_password": "",
    "smtp_use_tls": "1",
    "sender_email": "",
    "sender_name": "Fingerprint Release Team",
    "email_subject": "Your Fingerprint Release Form & Payment Code",
    "email_body": """Dear {first_name} {last_name},

Please find attached your Fingerprint Release Form for processing.

Your one-time IdentoGO payment code is: {code}

This code covers the cost of your fingerprint processing. When you visit the IdentoGO location, provide this code so that the fee is charged to our company account. Do NOT pay out of pocket.

Instructions:
1. Download and review the attached Fingerprint Release Form
2. Visit your assigned IdentoGO location
3. When prompted for payment, enter code: {code}
4. Complete the fingerprinting process

If you have any questions, please contact us.

Thank you.""",
    "release_form_path": "",
    "company_name": "",
    "ori_number": "",
    "auto_assign_codes": "0",
    "auto_send_email": "0",
}

def get_setting(db, key):
    row = db.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    if row:
        return row["value"]
    return DEFAULT_SETTINGS.get(key, "")

def set_setting(db, key, value):
    db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
    db.commit()

# ---------------------------------------------------------------------------
# XML Parsing
# ---------------------------------------------------------------------------
def parse_accio_xml(xml_string):
    applicants = []
    try:
        root = ET.fromstring(xml_string)
    except ET.ParseError as e:
        return applicants, str(e)

    # --- Format 1: ScreeningResults (completeOrder / placeOrder) ---
    for complete_order in root.iter("completeOrder"):
        order_number = complete_order.get("number", "")
        remote_number = complete_order.get("remote_number", "")
        for subject in complete_order.iter("subject"):
            first = _xt(subject, "name_first")
            last = _xt(subject, "name_last")
            email = _xt(subject, "email")
            phone = _xt(subject, "phone")
            if first or last:
                applicants.append(dict(first_name=first, last_name=last, email=email,
                                       phone=phone, accio_order_number=order_number,
                                       accio_remote_number=remote_number))

    for po in root.iter("placeOrder"):
        for subject in po.iter("subject"):
            first = _xt(subject, "name_first")
            last = _xt(subject, "name_last")
            email = _xt(subject, "email")
            phone = _xt(subject, "phone")
            if first or last:
                applicants.append(dict(first_name=first, last_name=last, email=email,
                                       phone=phone, accio_order_number=po.get("number", ""),
                                       accio_remote_number=""))

    # --- Format 2: Action Letter XML Post (postLetter with orderInfo) ---
    for post_letter in root.iter("postLetter"):
        order_number = post_letter.get("remote_order", "") or post_letter.get("order", "")
        remote_order = post_letter.get("remote_order", "")
        order_info = post_letter.find("orderInfo")
        if order_info is not None:
            first = _xt(order_info, "name_first")
            last = _xt(order_info, "name_last")
            email = _xt(order_info, "email")
            phone = _xt(order_info, "phone_number") or _xt(order_info, "phone")
            # Also check requester fields as fallback for contact info
            if not email:
                email = _xt(order_info, "requester_email")
            if not phone:
                phone = _xt(order_info, "requester_phone")
            if first or last:
                applicants.append(dict(first_name=first, last_name=last, email=email,
                                       phone=phone, accio_order_number=order_number,
                                       accio_remote_number=remote_order))

    # --- Format 3: Vendor dispatch XML (orderRequest with subject) ---
    for order_req in root.iter("orderRequest"):
        order_number = order_req.get("order", "") or order_req.get("number", "")
        remote_number = order_req.get("remote_order", "")
        for subject in order_req.iter("subject"):
            first = _xt(subject, "name_first") or _xt(subject, "firstName")
            last = _xt(subject, "name_last") or _xt(subject, "lastName")
            email = _xt(subject, "email") or _xt(subject, "InternetEmailAddress")
            phone = _xt(subject, "phone") or _xt(subject, "phone_number")
            if first or last:
                applicants.append(dict(first_name=first, last_name=last, email=email,
                                       phone=phone, accio_order_number=order_number,
                                       accio_remote_number=remote_number))

    # --- Format 4: Generic fallback - look for PersonalData or BackgroundSearchPackage ---
    if not applicants:
        for pd in root.iter("PersonalData"):
            pn = pd.find("PersonName")
            cm = pd.find("ContactMethod")
            first = ""
            last = ""
            email = ""
            phone = ""
            if pn is not None:
                first = _xt(pn, "GivenName") or _xt(pn, "name_first")
                last = _xt(pn, "FamilyName") or _xt(pn, "name_last")
            if cm is not None:
                email = _xt(cm, "InternetEmailAddress") or _xt(cm, "email")
                phone = _xt(cm, "FormattedNumber") or _xt(cm, "phone")
            if first or last:
                applicants.append(dict(first_name=first, last_name=last, email=email,
                                       phone=phone, accio_order_number="",
                                       accio_remote_number=""))

    return applicants, None

def _xt(el, tag):
    c = el.find(tag)
    return c.text.strip() if c is not None and c.text else ""

# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------
def send_release_email(db, applicant_id):
    a = db.execute("SELECT * FROM applicants WHERE id = ?", (applicant_id,)).fetchone()
    if not a: return False, "Applicant not found"
    if not a["email"]: return False, "No email address"
    if not a["assigned_code"]: return False, "No code assigned"

    smtp_server = get_setting(db, "smtp_server")
    smtp_port = int(get_setting(db, "smtp_port") or 587)
    smtp_user = get_setting(db, "smtp_username")
    smtp_pass = get_setting(db, "smtp_password")
    use_tls = get_setting(db, "smtp_use_tls") == "1"
    sender_email = get_setting(db, "sender_email")
    sender_name = get_setting(db, "sender_name")

    if not smtp_server or not sender_email:
        return False, "SMTP not configured. Go to Settings."

    reps = dict(first_name=a["first_name"], last_name=a["last_name"],
                email=a["email"], code=a["assigned_code"],
                company_name=get_setting(db, "company_name"),
                ori_number=get_setting(db, "ori_number"))

    subj = get_setting(db, "email_subject").format(**reps)
    body = get_setting(db, "email_body").format(**reps)

    msg = MIMEMultipart()
    msg["From"] = f"{sender_name} <{sender_email}>"
    msg["To"] = a["email"]
    msg["Subject"] = subj
    msg.attach(MIMEText(body, "plain"))

    rfp = get_setting(db, "release_form_path")
    if rfp and os.path.exists(rfp):
        with open(rfp, "rb") as f:
            part = MIMEBase("application", "pdf")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", 'attachment; filename="Fingerprint_Release_Form.pdf"')
            msg.attach(part)

    try:
        srv = smtplib.SMTP(smtp_server, smtp_port)
        if use_tls:
            srv.starttls()
        if smtp_user and smtp_pass:
            srv.login(smtp_user, smtp_pass)
        srv.send_message(msg)
        srv.quit()

        now = datetime.now().isoformat()
        db.execute("UPDATE applicants SET email_sent=1, email_sent_at=?, status='emailed', updated_at=? WHERE id=?", (now, now, applicant_id))
        db.execute("INSERT INTO email_log (applicant_id,recipient_email,subject,status) VALUES (?,?,?,'sent')", (applicant_id, a["email"], subj))
        db.commit()
        return True, "Email sent"
    except Exception as e:
        db.execute("INSERT INTO email_log (applicant_id,recipient_email,subject,status,error_message) VALUES (?,?,?,'failed',?)",
                   (applicant_id, a["email"], subj, str(e)))
        db.commit()
        return False, str(e)

def assign_code(db, applicant_id):
    a = db.execute("SELECT * FROM applicants WHERE id=?", (applicant_id,)).fetchone()
    if not a: return None, "Not found"
    if a["assigned_code"]: return a["assigned_code"], "Already assigned"

    code_row = db.execute("SELECT * FROM codes WHERE status='available' ORDER BY id LIMIT 1").fetchone()
    if not code_row: return None, "No codes available"

    now = datetime.now().isoformat()
    db.execute("UPDATE codes SET status='assigned', assigned_to=?, assigned_at=? WHERE id=?", (applicant_id, now, code_row["id"]))
    db.execute("UPDATE applicants SET assigned_code=?, status='code_assigned', updated_at=? WHERE id=?", (code_row["code"], now, applicant_id))
    db.commit()
    return code_row["code"], "Assigned"

# ---------------------------------------------------------------------------
# Bulk Code Import Engine
# ---------------------------------------------------------------------------
def import_codes_from_file(filepath, column_index=0, skip_header=True, batch_name=None):
    """
    Import codes from an Excel (.xlsx) or CSV file.
    Returns (imported_count, duplicate_count, error_message).
    Optimized for large batches (10,000+).
    """
    if batch_name is None:
        batch_name = os.path.basename(filepath)

    codes = []
    try:
        if filepath.endswith(".csv") or filepath.endswith(".txt"):
            with open(filepath, "r", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                for i, row in enumerate(reader):
                    if skip_header and i == 0:
                        continue
                    if len(row) > column_index and row[column_index].strip():
                        codes.append(row[column_index].strip())
        elif HAS_OPENPYXL and (filepath.endswith(".xlsx") or filepath.endswith(".xls")):
            wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
            ws = wb.active
            for i, row in enumerate(ws.iter_rows(values_only=True)):
                if skip_header and i == 0:
                    continue
                if len(row) > column_index and row[column_index] is not None:
                    val = str(row[column_index]).strip()
                    if val and val.lower() != "none":
                        codes.append(val)
            wb.close()
        else:
            return 0, 0, "Unsupported file format or openpyxl not installed"
    except Exception as e:
        return 0, 0, str(e)

    if not codes:
        return 0, 0, "No codes found in file"

    # Bulk insert with transaction for speed (important for 10,000+ codes)
    db = get_db()
    imported = 0
    duplicates = 0
    try:
        db.execute("BEGIN TRANSACTION")
        for code in codes:
            try:
                db.execute("INSERT INTO codes (code, batch_name) VALUES (?, ?)", (code, batch_name))
                imported += 1
            except sqlite3.IntegrityError:
                duplicates += 1
        db.execute("COMMIT")
    except Exception as e:
        db.execute("ROLLBACK")
        db.close()
        return imported, duplicates, str(e)

    db.close()
    logger.info(f"Imported {imported} codes ({duplicates} duplicates) from {batch_name}")
    return imported, duplicates, None


def auto_detect_code_column(filepath):
    """
    Automatically detect which column contains the codes by looking at headers
    and data patterns. Returns the column index (0-based).
    """
    try:
        if filepath.endswith(".csv") or filepath.endswith(".txt"):
            with open(filepath, "r", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if header:
                    for i, col in enumerate(header):
                        col_lower = col.lower().strip()
                        if any(kw in col_lower for kw in ["code", "voucher", "token", "payment", "ncac", "identogo"]):
                            return i
                # If no keyword match, check first data row for code-like patterns
                first_row = next(reader, None)
                if first_row:
                    for i, val in enumerate(first_row):
                        val = str(val).strip()
                        if len(val) >= 6 and any(c.isalpha() for c in val) and any(c.isdigit() for c in val):
                            return i
        elif HAS_OPENPYXL:
            wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
            ws = wb.active
            rows = list(ws.iter_rows(values_only=True, max_row=3))
            wb.close()
            if rows:
                header = rows[0]
                for i, col in enumerate(header):
                    if col:
                        col_lower = str(col).lower().strip()
                        if any(kw in col_lower for kw in ["code", "voucher", "token", "payment", "ncac", "identogo"]):
                            return i
                # Check data row for alphanumeric patterns
                if len(rows) > 1:
                    for i, val in enumerate(rows[1]):
                        if val:
                            val = str(val).strip()
                            if len(val) >= 6 and any(c.isalpha() for c in val) and any(c.isdigit() for c in val):
                                return i
    except Exception:
        pass
    return 0  # Default to first column


# ---------------------------------------------------------------------------
# Folder Watcher (background thread)
# ---------------------------------------------------------------------------
import threading
import time

_watcher_running = False

def start_folder_watcher():
    """Start a background thread that watches the 'watch' folder for new Excel/CSV files."""
    global _watcher_running
    if _watcher_running:
        return
    _watcher_running = True

    def watcher_loop():
        logger.info(f"Folder watcher started. Watching: {WATCH_FOLDER}")
        logger.info(f"Drop .xlsx or .csv files here for automatic import.")
        while _watcher_running:
            try:
                for fname in os.listdir(WATCH_FOLDER):
                    fpath = os.path.join(WATCH_FOLDER, fname)
                    if not os.path.isfile(fpath):
                        continue
                    if not fname.lower().endswith((".xlsx", ".xls", ".csv", ".txt")):
                        continue
                    # Skip very recently modified files (still being copied)
                    if time.time() - os.path.getmtime(fpath) < 2:
                        continue

                    logger.info(f"Auto-import: Found new file '{fname}'")
                    col = auto_detect_code_column(fpath)
                    logger.info(f"Auto-import: Detected code column index {col}")

                    batch_name = f"Auto: {fname} ({datetime.now().strftime('%Y-%m-%d %H:%M')})"
                    imported, dups, err = import_codes_from_file(
                        fpath, column_index=col, skip_header=True, batch_name=batch_name
                    )

                    if err:
                        logger.error(f"Auto-import error for '{fname}': {err}")
                    else:
                        logger.info(f"Auto-import complete: {imported} codes imported, {dups} duplicates from '{fname}'")

                    # Move to processed folder
                    dest = os.path.join(PROCESSED_FOLDER, f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{fname}")
                    shutil.move(fpath, dest)
                    logger.info(f"Auto-import: Moved '{fname}' to processed/")

            except Exception as e:
                logger.error(f"Folder watcher error: {e}")

            time.sleep(5)  # Check every 5 seconds

    t = threading.Thread(target=watcher_loop, daemon=True)
    t.start()


# ---------------------------------------------------------------------------
# HTML Templates (inline for portability)
# ---------------------------------------------------------------------------
def h(text):
    """HTML-escape a string."""
    if text is None: return ""
    return str(text).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace('"',"&quot;")

def render_page(title, content, active=""):
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{h(title)} - Fingerprint Release Manager</title>
<link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css" rel="stylesheet">
<style>
:root{{--p:#2563eb;--pd:#1d4ed8;--s:#16a34a;--w:#d97706;--d:#dc2626;--g50:#f9fafb;--g100:#f3f4f6;--g200:#e5e7eb;--g300:#d1d5db;--g500:#6b7280;--g700:#374151;--g900:#111827;}}
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:var(--g50);color:var(--g900);line-height:1.5;}}
.sb{{position:fixed;top:0;left:0;bottom:0;width:220px;background:var(--g900);color:#fff;padding:20px 0;z-index:100;}}
.sb-brand{{padding:0 16px 16px;border-bottom:1px solid rgba(255,255,255,.1);margin-bottom:8px;}}
.sb-brand h2{{font-size:15px;display:flex;align-items:center;gap:8px;}}
.sb-brand span{{color:var(--g500);font-size:11px;display:block;margin-top:2px;}}
.nl{{display:flex;align-items:center;gap:10px;padding:9px 16px;color:var(--g300);text-decoration:none;font-size:13px;transition:.15s;}}
.nl:hover{{background:rgba(255,255,255,.08);color:#fff;}}.nl.ac{{background:rgba(37,99,235,.3);color:#fff;border-right:3px solid var(--p);}}
.nl i{{width:18px;text-align:center;}}
.mc{{margin-left:220px;padding:20px 28px;min-height:100vh;}}
.ph{{display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;flex-wrap:wrap;gap:12px;}}
.ph h1{{font-size:22px;font-weight:700;}}
.fl{{padding:10px 14px;border-radius:8px;margin-bottom:12px;font-size:13px;display:flex;align-items:center;gap:8px;}}
.fl-s{{background:#dcfce7;color:#166534;border:1px solid #bbf7d0;}}.fl-e{{background:#fef2f2;color:#991b1b;border:1px solid #fecaca;}}
.cd{{background:#fff;border-radius:10px;border:1px solid var(--g200);box-shadow:0 1px 2px rgba(0,0,0,.04);margin-bottom:16px;}}
.cd-h{{padding:14px 18px;border-bottom:1px solid var(--g200);display:flex;justify-content:space-between;align-items:center;}}
.cd-h h3{{font-size:15px;font-weight:600;}}.cd-b{{padding:18px;}}
.sg{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:20px;}}
.sc{{background:#fff;border-radius:10px;padding:16px;border:1px solid var(--g200);}}
.sc .sv{{font-size:26px;font-weight:700;margin-bottom:2px;}}.sc .sl{{font-size:12px;color:var(--g500);}}
.sc.sp .sv{{color:var(--p);}}.sc.ss .sv{{color:var(--s);}}.sc.sw .sv{{color:var(--w);}}
table{{width:100%;border-collapse:collapse;}}
th{{text-align:left;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.05em;color:var(--g500);padding:8px 14px;background:var(--g50);border-bottom:1px solid var(--g200);}}
td{{padding:10px 14px;border-bottom:1px solid var(--g100);font-size:13px;}}
tr:hover td{{background:var(--g50);}}
.btn{{display:inline-flex;align-items:center;gap:5px;padding:7px 14px;border-radius:7px;font-size:12px;font-weight:500;border:none;cursor:pointer;text-decoration:none;transition:.15s;}}
.btn-p{{background:var(--p);color:#fff;}}.btn-p:hover{{background:var(--pd);}}
.btn-s{{background:var(--s);color:#fff;}}.btn-s:hover{{background:#15803d;}}
.btn-w{{background:var(--w);color:#fff;}}.btn-o{{background:#fff;color:var(--g700);border:1px solid var(--g300);}}.btn-o:hover{{background:var(--g50);}}
.btn-sm{{padding:4px 9px;font-size:11px;}}
.badge{{display:inline-block;padding:2px 8px;border-radius:20px;font-size:10px;font-weight:600;text-transform:uppercase;}}
.b-pe{{background:#fef3c7;color:#92400e;}}.b-ca{{background:#dbeafe;color:#1e40af;}}.b-em{{background:#dcfce7;color:#166534;}}.b-av{{background:#dcfce7;color:#166534;}}.b-as{{background:#dbeafe;color:#1e40af;}}
.fg{{margin-bottom:14px;}}.fg label{{display:block;font-size:12px;font-weight:600;margin-bottom:4px;color:var(--g700);}}
.fg input,.fg select,.fg textarea{{width:100%;padding:8px 10px;border:1px solid var(--g300);border-radius:7px;font-size:13px;font-family:inherit;}}
.fg textarea{{min-height:100px;resize:vertical;}}.fg .ht{{font-size:11px;color:var(--g500);margin-top:3px;}}
.fr{{display:grid;grid-template-columns:1fr 1fr;gap:14px;}}
.es{{text-align:center;padding:40px 20px;color:var(--g500);}}.es i{{font-size:40px;margin-bottom:12px;display:block;}}
.acts{{display:flex;gap:5px;}}.api-box{{background:var(--g900);color:#10b981;padding:10px 14px;border-radius:7px;font-family:monospace;font-size:12px;margin-top:6px;word-break:break-all;}}
hr.div{{border:none;border-top:1px solid var(--g200);margin:18px 0;}}
@media(max-width:768px){{.sb{{display:none;}}.mc{{margin-left:0;padding:14px;}}.fr{{grid-template-columns:1fr;}}.sg{{grid-template-columns:repeat(2,1fr);}}}}
</style>
</head>
<body>
<nav class="sb">
<div class="sb-brand"><h2><i class="fas fa-fingerprint"></i> FP Release</h2><span>Fingerprint Release Manager</span></div>
<a href="/" class="nl {"ac" if active=="dash" else ""}"><i class="fas fa-chart-line"></i> Dashboard</a>
<a href="/applicants" class="nl {"ac" if active=="app" else ""}"><i class="fas fa-users"></i> Applicants</a>
<a href="/codes" class="nl {"ac" if active=="codes" else ""}"><i class="fas fa-key"></i> Payment Codes</a>
<a href="/logs" class="nl {"ac" if active=="logs" else ""}"><i class="fas fa-file-code"></i> Logs</a>
<a href="/settings" class="nl {"ac" if active=="set" else ""}"><i class="fas fa-cog"></i> Settings</a>
</nav>
<main class="mc">{content}</main>
</body></html>"""

# ---------------------------------------------------------------------------
# Flash message helper (stored in a module-level list, cleared on read)
# ---------------------------------------------------------------------------
_flash_messages = []

def flash(msg, cat="success"):
    _flash_messages.append((cat, msg))

def render_flashes():
    out = ""
    while _flash_messages:
        cat, msg = _flash_messages.pop(0)
        icon = "check-circle" if cat == "success" else "exclamation-circle"
        cls = "fl-s" if cat == "success" else "fl-e"
        out += f'<div class="fl {cls}"><i class="fas fa-{icon}"></i> {h(msg)}</div>'
    return out

# ---------------------------------------------------------------------------
# Page renderers
# ---------------------------------------------------------------------------
def page_dashboard(db):
    stats = {
        "total": db.execute("SELECT COUNT(*) FROM applicants").fetchone()[0],
        "pending": db.execute("SELECT COUNT(*) FROM applicants WHERE status='pending'").fetchone()[0],
        "assigned": db.execute("SELECT COUNT(*) FROM applicants WHERE status='code_assigned'").fetchone()[0],
        "emailed": db.execute("SELECT COUNT(*) FROM applicants WHERE status='emailed'").fetchone()[0],
        "avail_codes": db.execute("SELECT COUNT(*) FROM codes WHERE status='available'").fetchone()[0],
        "used_codes": db.execute("SELECT COUNT(*) FROM codes WHERE status='assigned'").fetchone()[0],
    }
    rows = db.execute("SELECT * FROM applicants ORDER BY created_at DESC LIMIT 25").fetchall()

    c = render_flashes()
    c += f"""<div class="ph"><h1>Dashboard</h1>
    <div class="acts"><a href="/applicants/add" class="btn btn-p"><i class="fas fa-plus"></i> Add Applicant</a>
    <a href="/codes/import" class="btn btn-o"><i class="fas fa-upload"></i> Import Codes</a></div></div>"""

    c += f"""<div class="sg">
    <div class="sc sp"><div class="sv">{stats["total"]}</div><div class="sl">Total Applicants</div></div>
    <div class="sc sw"><div class="sv">{stats["pending"]}</div><div class="sl">Pending</div></div>
    <div class="sc"><div class="sv">{stats["assigned"]}</div><div class="sl">Code Assigned</div></div>
    <div class="sc ss"><div class="sv">{stats["emailed"]}</div><div class="sl">Email Sent</div></div>
    <div class="sc ss"><div class="sv">{stats["avail_codes"]}</div><div class="sl">Available Codes</div></div>
    <div class="sc"><div class="sv">{stats["used_codes"]}</div><div class="sl">Used Codes</div></div>
    </div>"""

    if stats["pending"] > 0 and stats["avail_codes"] > 0:
        c += f"""<div class="cd" style="border-left:4px solid var(--w)"><div class="cd-b" style="display:flex;justify-content:space-between;align-items:center">
        <div><strong>{stats["pending"]} applicant(s) waiting</strong><p style="font-size:12px;color:var(--g500)">Click Bulk Process to assign codes & send emails to all pending applicants.</p></div>
        <form action="/applicants/bulk-process" method="POST"><button class="btn btn-w"><i class="fas fa-bolt"></i> Bulk Process</button></form></div></div>"""

    if stats["avail_codes"] == 0:
        c += '<div class="cd" style="border-left:4px solid var(--d)"><div class="cd-b"><strong>No codes available!</strong> <a href="/codes/import">Import codes</a> from IdentoGO.</div></div>'
    elif stats["avail_codes"] < 50:
        c += f'<div class="cd" style="border-left:4px solid var(--w)"><div class="cd-b"><strong>Low inventory:</strong> {stats["avail_codes"]} codes left. <a href="/codes/import">Import more</a>.</div></div>'

    c += '<div class="cd"><div class="cd-h"><h3>Recent Applicants</h3><a href="/applicants" class="btn btn-sm btn-o">View All</a></div>'
    if rows:
        c += _applicant_table(rows)
    else:
        c += '<div class="es"><i class="fas fa-inbox"></i><h3>No applicants yet</h3><p>They appear here from Accio Data or manual entry.</p></div>'
    c += '</div>'
    return render_page("Dashboard", c, "dash")

def page_applicants(db, params):
    sf = params.get("status", ["all"])[0]
    search = params.get("search", [""])[0]

    q = "SELECT * FROM applicants"
    p = []
    conds = []
    if sf != "all":
        conds.append("status=?"); p.append(sf)
    if search:
        conds.append("(first_name LIKE ? OR last_name LIKE ? OR email LIKE ? OR accio_order_number LIKE ?)")
        p.extend([f"%{search}%"]*4)
    if conds:
        q += " WHERE " + " AND ".join(conds)
    q += " ORDER BY created_at DESC"
    rows = db.execute(q, p).fetchall()

    c = render_flashes()
    c += f"""<div class="ph"><h1>Applicants</h1><div class="acts">
    <form action="/applicants/bulk-process" method="POST" style="display:inline"><button class="btn btn-w"><i class="fas fa-bolt"></i> Bulk Process</button></form>
    <a href="/applicants/add" class="btn btn-p"><i class="fas fa-plus"></i> Add Applicant</a></div></div>"""

    sel = lambda v: "selected" if sf==v else ""
    c += f"""<div style="display:flex;gap:10px;margin-bottom:14px;flex-wrap:wrap"><form method="GET" style="display:flex;gap:10px;align-items:center;flex-wrap:wrap">
    <select name="status" onchange="this.form.submit()" style="padding:7px 10px;border:1px solid var(--g300);border-radius:7px;font-size:12px">
    <option value="all" {sel("all")}>All</option><option value="pending" {sel("pending")}>Pending</option>
    <option value="code_assigned" {sel("code_assigned")}>Code Assigned</option><option value="emailed" {sel("emailed")}>Emailed</option></select>
    <input name="search" placeholder="Search..." value="{h(search)}" style="padding:7px 10px;border:1px solid var(--g300);border-radius:7px;font-size:12px;min-width:220px">
    <button class="btn btn-sm btn-o"><i class="fas fa-search"></i></button></form></div>"""

    c += '<div class="cd">'
    if rows:
        c += _applicant_table(rows, full=True)
    else:
        c += '<div class="es"><i class="fas fa-users"></i><h3>No applicants found</h3></div>'
    c += '</div>'
    return render_page("Applicants", c, "app")

def _applicant_table(rows, full=False):
    t = '<table><thead><tr><th>Name</th><th>Email</th><th>Phone</th><th>Order #</th><th>Status</th><th>Code</th>'
    if full: t += '<th>Email Sent</th>'
    t += '<th>Actions</th></tr></thead><tbody>'
    for a in rows:
        sid = a["id"]
        badge_cls = {"pending":"b-pe","code_assigned":"b-ca","emailed":"b-em"}.get(a["status"],"b-pe")
        status_label = a["status"].replace("_"," ").title()
        t += f'<tr><td><strong>{h(a["first_name"])} {h(a["last_name"])}</strong></td>'
        t += f'<td>{h(a["email"]) or "—"}</td><td>{h(a["phone"]) or "—"}</td>'
        t += f'<td>{h(a["accio_order_number"]) or "—"}</td>'
        t += f'<td><span class="badge {badge_cls}">{status_label}</span></td>'
        t += f'<td style="font-family:monospace;font-size:12px">{h(a["assigned_code"]) or "—"}</td>'
        if full:
            if a["email_sent"]:
                t += f'<td style="color:var(--s);font-size:12px"><i class="fas fa-check-circle"></i> {(a["email_sent_at"] or "")[:16]}</td>'
            else:
                t += '<td>—</td>'
        t += '<td><div class="acts">'
        if a["status"] == "pending" and a["email"]:
            t += f'<form action="/applicants/{sid}/assign-and-send" method="POST" style="display:inline"><button class="btn btn-sm btn-s" title="Assign & Send"><i class="fas fa-paper-plane"></i> Process</button></form>'
        elif a["status"] == "pending":
            t += f'<form action="/applicants/{sid}/assign-code" method="POST" style="display:inline"><button class="btn btn-sm btn-p" title="Assign code"><i class="fas fa-key"></i></button></form>'
        elif a["status"] == "code_assigned" and a["email"]:
            t += f'<form action="/applicants/{sid}/send-email" method="POST" style="display:inline"><button class="btn btn-sm btn-s" title="Send email"><i class="fas fa-envelope"></i></button></form>'
        elif a["status"] == "emailed":
            t += '<span style="color:var(--s);font-size:11px"><i class="fas fa-check"></i> Done</span>'
        t += '</div></td></tr>'
    t += '</tbody></table>'
    return t

def page_add_applicant():
    c = render_flashes()
    c += """<div class="ph"><h1>Add Applicant</h1></div>
    <div class="cd"><div class="cd-b"><form method="POST" action="/applicants/add">
    <div class="fr"><div class="fg"><label>First Name *</label><input name="first_name" required></div>
    <div class="fg"><label>Last Name *</label><input name="last_name" required></div></div>
    <div class="fr"><div class="fg"><label>Email</label><input type="email" name="email"><div class="ht">Required for email delivery.</div></div>
    <div class="fg"><label>Phone</label><input name="phone"></div></div>
    <div class="fr"><div class="fg"><label>Accio Order #</label><input name="accio_order_number"></div>
    <div class="fg"><label>Notes</label><input name="notes"></div></div>
    <div style="display:flex;gap:10px;margin-top:8px"><button class="btn btn-p"><i class="fas fa-plus"></i> Add</button>
    <a href="/applicants" class="btn btn-o">Cancel</a></div></form></div></div>"""
    return render_page("Add Applicant", c, "app")

def page_codes(db, params):
    sf = params.get("status", ["all"])[0]
    q = """SELECT codes.*, applicants.first_name, applicants.last_name
           FROM codes LEFT JOIN applicants ON codes.assigned_to=applicants.id"""
    p = []
    if sf != "all":
        q += " WHERE codes.status=?"; p.append(sf)
    q += " ORDER BY codes.id DESC"
    rows = db.execute(q, p).fetchall()
    stats = {
        "total": db.execute("SELECT COUNT(*) FROM codes").fetchone()[0],
        "avail": db.execute("SELECT COUNT(*) FROM codes WHERE status='available'").fetchone()[0],
        "assigned": db.execute("SELECT COUNT(*) FROM codes WHERE status='assigned'").fetchone()[0],
    }

    c = render_flashes()
    c += f"""<div class="ph"><h1>Payment Codes</h1>
    <a href="/codes/import" class="btn btn-p"><i class="fas fa-file-excel"></i> Import from Excel</a></div>"""

    c += f"""<div class="sg">
    <div class="sc ss"><div class="sv">{stats["avail"]}</div><div class="sl">Available</div></div>
    <div class="sc"><div class="sv">{stats["assigned"]}</div><div class="sl">Assigned</div></div>
    <div class="sc sp"><div class="sv">{stats["total"]}</div><div class="sl">Total</div></div></div>"""

    # Auto-import folder info
    watch_abs = os.path.abspath(WATCH_FOLDER)
    processed_files = len([f for f in os.listdir(PROCESSED_FOLDER) if os.path.isfile(os.path.join(PROCESSED_FOLDER, f))]) if os.path.exists(PROCESSED_FOLDER) else 0
    c += f"""<div class="cd" style="border-left:4px solid var(--s)"><div class="cd-h"><h3><i class="fas fa-magic"></i> Auto-Import (Drop &amp; Go)</h3></div><div class="cd-b">
    <p style="font-size:13px;margin-bottom:10px">Drop your IdentoGO Excel file into this folder and codes are imported <strong>automatically</strong> — no clicking required:</p>
    <div class="api-box" style="user-select:all">{h(watch_abs)}</div>
    <p style="font-size:12px;color:var(--g500);margin-top:8px"><i class="fas fa-check-circle" style="color:var(--s)"></i> The app auto-detects which column has the codes.
    Processed files are moved to <code>watch/processed/</code>. ({processed_files} file(s) processed so far)</p>
    <hr class="div">
    <p style="font-size:12px;color:var(--g500)"><strong>API option:</strong> You can also push codes via API:</p>
    <div class="api-box">curl -X POST http://localhost:{PORT}/api/codes -H "Content-Type: application/json" -d '{{"codes":["CODE1","CODE2",...]}}'</div>
    </div></div>"""

    # Manual add
    c += """<div class="cd"><div class="cd-h"><h3>Quick Add Codes</h3></div><div class="cd-b">
    <form action="/codes/add-manual" method="POST"><div class="fr">
    <div class="fg"><label>Paste Codes (one per line)</label><textarea name="codes" rows="3" placeholder="ABC123&#10;DEF456"></textarea></div>
    <div class="fg"><label>Batch Label</label><input name="batch_name" value="Manual Entry">
    <button class="btn btn-p" style="margin-top:12px"><i class="fas fa-plus"></i> Add Codes</button></div></div></form></div></div>"""

    sel = lambda v: "selected" if sf==v else ""
    c += f"""<div class="cd"><div class="cd-h"><h3>All Codes</h3>
    <form method="GET"><select name="status" onchange="this.form.submit()" style="padding:6px 8px;border:1px solid var(--g300);border-radius:6px;font-size:12px">
    <option value="all" {sel("all")}>All</option><option value="available" {sel("available")}>Available</option>
    <option value="assigned" {sel("assigned")}>Assigned</option></select></form></div>"""

    if rows:
        c += '<table><thead><tr><th>Code</th><th>Status</th><th>Assigned To</th><th>Batch</th><th>Imported</th></tr></thead><tbody>'
        for r in rows:
            st_cls = "b-av" if r["status"]=="available" else "b-as"
            st_lbl = r["status"].title()
            name = f'{r["first_name"]} {r["last_name"]}' if r["first_name"] else "—"
            c += f'<tr><td style="font-family:monospace;font-weight:600">{h(r["code"])}</td>'
            c += f'<td><span class="badge {st_cls}">{st_lbl}</span></td>'
            c += f'<td>{h(name)}</td><td>{h(r["batch_name"]) or "—"}</td>'
            c += f'<td>{(r["imported_at"] or "")[:16] or "—"}</td></tr>'
        c += '</tbody></table>'
    else:
        c += '<div class="es"><i class="fas fa-key"></i><h3>No codes</h3><p>Import from Excel or add manually.</p></div>'
    c += '</div>'
    return render_page("Payment Codes", c, "codes")

def page_import_codes():
    c = render_flashes()
    c += """<div class="ph"><h1>Import Codes from Excel</h1></div>
    <div class="cd"><div class="cd-b"><form method="POST" action="/codes/import" enctype="multipart/form-data">
    <div class="fg"><label>Select Excel or CSV File</label><input type="file" name="file" accept=".xlsx,.xls,.csv" required>
    <div class="ht">Upload the IdentoGO spreadsheet with payment codes.</div></div>
    <div class="fr"><div class="fg"><label>Column with Codes</label>
    <select name="column_index"><option value="0">A (1st)</option><option value="1">B (2nd)</option>
    <option value="2">C (3rd)</option><option value="3">D (4th)</option><option value="4">E (5th)</option></select></div>
    <div class="fg"><label>Batch Label</label><input name="batch_name" placeholder="e.g. IdentoGO March 2026"></div></div>
    <div class="fg"><label style="display:flex;align-items:center;gap:6px;font-weight:normal;cursor:pointer">
    <input type="checkbox" name="skip_header" checked style="width:auto"> Skip header row</label></div>
    <div style="display:flex;gap:10px"><button class="btn btn-p"><i class="fas fa-upload"></i> Import</button>
    <a href="/codes" class="btn btn-o">Cancel</a></div></form></div></div>"""
    return render_page("Import Codes", c, "codes")

def page_settings(db):
    s = {k: get_setting(db, k) for k in DEFAULT_SETTINGS}
    c = render_flashes()
    c += '<div class="ph"><h1>Settings</h1></div>'
    c += f"""<form method="POST" action="/settings" enctype="multipart/form-data">
    <div class="cd"><div class="cd-h"><h3><i class="fas fa-database"></i> Accio Data API</h3></div><div class="cd-b">
    <p style="font-size:12px;color:var(--g500);margin-bottom:12px">Credentials from your Accio Data admin account.</p>
    <div class="fr"><div class="fg"><label>Account</label><input name="accio_account" value="{h(s["accio_account"])}"></div>
    <div class="fg"><label>Username</label><input name="accio_username" value="{h(s["accio_username"])}"></div></div>
    <div class="fr"><div class="fg"><label>Password</label><input type="password" name="accio_password" value="{h(s["accio_password"])}"></div>
    <div class="fg"><label>Accio Post URL</label><input name="accio_post_url" value="{h(s["accio_post_url"])}"></div></div>
    <hr class="div"><h4 style="font-size:13px;margin-bottom:6px">Your Push Endpoint</h4>
    <p style="font-size:12px;color:var(--g500)">Set this URL in Accio Data (XML Post URL for Action Letters or XMLresults_post_url):</p>
    <div class="api-box">https://fingerprint-release-manager1.onrender.com/api/accio-push</div>
    <div class="ht" style="margin-top:8px">Supports: ScreeningResults XML, Action Letter XML Post (postLetter), Vendor dispatch XML, and HR-XML PersonalData formats.</div></div></div>

    <div class="cd"><div class="cd-h"><h3><i class="fas fa-building"></i> Company</h3></div><div class="cd-b">
    <div class="fr"><div class="fg"><label>Company Name</label><input name="company_name" value="{h(s["company_name"])}"></div>
    <div class="fg"><label>ORI Number</label><input name="ori_number" value="{h(s["ori_number"])}"></div></div></div></div>

    <div class="cd"><div class="cd-h"><h3><i class="fas fa-envelope"></i> Email (SMTP)</h3></div><div class="cd-b">
    <div class="fr"><div class="fg"><label>SMTP Server</label><input name="smtp_server" value="{h(s["smtp_server"])}" placeholder="smtp.gmail.com"></div>
    <div class="fg"><label>Port</label><input type="number" name="smtp_port" value="{h(s["smtp_port"])}"></div></div>
    <div class="fr"><div class="fg"><label>SMTP Username</label><input name="smtp_username" value="{h(s["smtp_username"])}"></div>
    <div class="fg"><label>SMTP Password</label><input type="password" name="smtp_password" value="{h(s["smtp_password"])}"></div></div>
    <div class="fr"><div class="fg"><label>Sender Email</label><input name="sender_email" value="{h(s["sender_email"])}"></div>
    <div class="fg"><label>Sender Name</label><input name="sender_name" value="{h(s["sender_name"])}"></div></div>
    <div class="fg"><label style="display:flex;align-items:center;gap:6px;font-weight:normal;cursor:pointer">
    <input type="hidden" name="smtp_use_tls" value="0">
    <input type="checkbox" name="smtp_use_tls" value="1" {"checked" if s["smtp_use_tls"]=="1" else ""} style="width:auto"> Use TLS</label></div></div></div>

    <div class="cd"><div class="cd-h"><h3><i class="fas fa-file-pdf"></i> Release Form PDF</h3></div><div class="cd-b">
    <div class="fg"><label>Upload Release Form</label><input type="file" name="release_form_file" accept=".pdf">"""
    if s["release_form_path"]:
        c += f'<div class="ht" style="color:var(--s)"><i class="fas fa-check-circle"></i> Uploaded: {os.path.basename(s["release_form_path"])}</div>'
    else:
        c += '<div class="ht">Upload your fingerprint release form PDF to attach to emails.</div>'
    c += f"""</div></div></div>

    <div class="cd"><div class="cd-h"><h3><i class="fas fa-robot"></i> Automation</h3></div><div class="cd-b">
    <div class="fg"><label style="display:flex;align-items:center;gap:6px;font-weight:normal;cursor:pointer">
    <input type="hidden" name="auto_assign_codes" value="0">
    <input type="checkbox" name="auto_assign_codes" value="1" {"checked" if s["auto_assign_codes"]=="1" else ""} style="width:auto"> Auto-assign payment code when applicant is received from Accio</label>
    <div class="ht">When enabled, the next available IdentoGO code is automatically assigned to each new applicant received via XML push.</div></div>
    <div class="fg"><label style="display:flex;align-items:center;gap:6px;font-weight:normal;cursor:pointer">
    <input type="hidden" name="auto_send_email" value="0">
    <input type="checkbox" name="auto_send_email" value="1" {"checked" if s["auto_send_email"]=="1" else ""} style="width:auto"> Auto-send release email after code assignment</label>
    <div class="ht">When enabled, the fingerprint release email (with PDF and payment code) is sent automatically. Requires auto-assign to also be on and SMTP to be configured.</div></div></div></div>

    <div class="cd"><div class="cd-h"><h3><i class="fas fa-edit"></i> Email Template</h3></div><div class="cd-b">
    <div class="fg"><label>Subject</label><input name="email_subject" value="{h(s["email_subject"])}">
    <div class="ht">Variables: {{first_name}} {{last_name}} {{code}} {{company_name}} {{ori_number}}</div></div>
    <div class="fg"><label>Body</label><textarea name="email_body" rows="12">{h(s["email_body"])}</textarea>
    <div class="ht">Variables: {{first_name}} {{last_name}} {{email}} {{code}} {{company_name}} {{ori_number}}</div></div></div></div>

    <button class="btn btn-p" style="padding:10px 22px;margin-bottom:16px"><i class="fas fa-save"></i> Save All Settings</button></form>

    <div class="cd"><div class="cd-h"><h3><i class="fas fa-vial"></i> Test Email</h3></div><div class="cd-b">
    <form action="/settings/test-email" method="POST" style="display:flex;gap:10px;align-items:flex-end">
    <div class="fg" style="flex:1;margin-bottom:0"><label>Send To</label><input type="email" name="test_email" value="{h(s["sender_email"])}"></div>
    <button class="btn btn-s" style="height:38px"><i class="fas fa-paper-plane"></i> Test</button></form></div></div>"""
    return render_page("Settings", c, "set")

def page_logs(db):
    xlogs = db.execute("SELECT * FROM xml_log ORDER BY received_at DESC LIMIT 50").fetchall()
    elogs = db.execute("""SELECT email_log.*, applicants.first_name, applicants.last_name
        FROM email_log LEFT JOIN applicants ON email_log.applicant_id=applicants.id
        ORDER BY email_log.sent_at DESC LIMIT 50""").fetchall()

    c = render_flashes()
    c += '<div class="ph"><h1>Activity Logs</h1></div>'

    c += '<div class="cd"><div class="cd-h"><h3><i class="fas fa-envelope"></i> Email Log</h3></div>'
    if elogs:
        c += '<table><thead><tr><th>Applicant</th><th>To</th><th>Subject</th><th>Status</th><th>Time</th></tr></thead><tbody>'
        for l in elogs:
            nm = f'{l["first_name"]} {l["last_name"]}' if l["first_name"] else "—"
            st = '<span class="badge b-av">Sent</span>' if l["status"]=="sent" else '<span class="badge" style="background:#fef2f2;color:#991b1b">Failed</span>'
            c += f'<tr><td>{h(nm)}</td><td>{h(l["recipient_email"])}</td><td style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{h(l["subject"])}</td><td>{st}</td><td>{(l["sent_at"] or "")[:16]}</td></tr>'
        c += '</tbody></table>'
    else:
        c += '<div class="es"><p>No emails sent yet.</p></div>'
    c += '</div>'

    c += '<div class="cd"><div class="cd-h"><h3><i class="fas fa-file-code"></i> XML Log</h3></div>'
    if xlogs:
        c += '<table><thead><tr><th>Direction</th><th>Status</th><th>Time</th><th>Preview</th></tr></thead><tbody>'
        for l in xlogs:
            st_cls = "b-av" if l["parsed_status"]=="success" else "b-pe"
            c += f'<tr><td><span class="badge b-pe">{h(l["direction"])}</span></td><td><span class="badge {st_cls}">{h(l["parsed_status"])}</span></td>'
            c += f'<td>{(l["received_at"] or "")[:16]}</td><td style="font-size:10px;font-family:monospace;max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{h((l["raw_xml"] or "")[:80])}</td></tr>'
        c += '</tbody></table>'
    else:
        c += '<div class="es"><p>No XML data received yet.</p></div>'
    c += '</div>'
    return render_page("Logs", c, "logs")

# ---------------------------------------------------------------------------
# HTTP Handler
# ---------------------------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        logger.info(f"{self.client_address[0]} - {fmt % args}")

    def _send(self, code, body, ct="text/html"):
        self.send_response(code)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", str(len(body.encode())))
        self.end_headers()
        self.wfile.write(body.encode())

    def _redirect(self, url):
        self.send_response(303)
        self.send_header("Location", url)
        self.end_headers()

    def _parse_form(self):
        ct = self.headers.get("Content-Type", "")
        length = int(self.headers.get("Content-Length", 0))
        if "multipart/form-data" in ct:
            env = {"REQUEST_METHOD": "POST", "CONTENT_TYPE": ct, "CONTENT_LENGTH": str(length)}
            fs = cgi.FieldStorage(fp=self.rfile, headers=self.headers, environ=env)
            return fs
        else:
            body = self.rfile.read(length).decode()
            return urllib.parse.parse_qs(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        params = urllib.parse.parse_qs(parsed.query)
        db = get_db()

        try:
            if path == "/":
                self._send(200, page_dashboard(db))
            elif path == "/applicants":
                self._send(200, page_applicants(db, params))
            elif path == "/applicants/add":
                self._send(200, page_add_applicant())
            elif path == "/codes":
                self._send(200, page_codes(db, params))
            elif path == "/codes/import":
                self._send(200, page_import_codes())
            elif path == "/settings":
                self._send(200, page_settings(db))
            elif path == "/logs":
                self._send(200, page_logs(db))
            else:
                self._send(404, render_page("Not Found", '<div class="es"><i class="fas fa-exclamation-triangle"></i><h3>Page not found</h3></div>'))
        finally:
            db.close()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        db = get_db()

        try:
            # Accio Data XML push endpoint
            if path == "/api/accio-push":
                # Validate Basic Auth credentials from Accio
                ACCIO_USERNAME = os.environ.get("ACCIO_USERNAME", "FPrelease")
                ACCIO_PASSWORD = os.environ.get("ACCIO_PASSWORD", "fingerprint")
                auth_header = self.headers.get("Authorization", "")
                auth_valid = False
                if auth_header.startswith("Basic "):
                    try:
                        decoded = base64.b64decode(auth_header[6:]).decode()
                        u, p = decoded.split(":", 1)
                        if u == ACCIO_USERNAME and p == ACCIO_PASSWORD:
                            auth_valid = True
                    except Exception:
                        pass
                # Also check for credentials in XML or query params as fallback
                qs = urllib.parse.parse_qs(parsed.query)
                if not auth_valid and qs.get("username", [None])[0] == ACCIO_USERNAME and qs.get("password", [None])[0] == ACCIO_PASSWORD:
                    auth_valid = True
                if not auth_valid:
                    self._send(401, '<?xml version="1.0" encoding="UTF-8"?>\n<XML><error>Authentication required</error></XML>', "text/xml")
                    return
                length = int(self.headers.get("Content-Length", 0))
                raw = self.rfile.read(length).decode()
                db.execute("INSERT INTO xml_log (direction,raw_xml,parsed_status) VALUES ('inbound',?,'processing')", (raw[:10000],))
                db.commit()
                applicants_data, err = parse_accio_xml(raw)
                if err:
                    db.execute("UPDATE xml_log SET parsed_status='error',error_message=? WHERE id=(SELECT MAX(id) FROM xml_log)", (err,))
                    db.commit()
                    self._send(400, '<?xml version="1.0" encoding="UTF-8"?>\n<XML><error>XML parse error</error></XML>', "text/xml")
                    return
                added = 0
                auto_assign = get_setting(db, "auto_assign_codes") == "1"
                auto_email = get_setting(db, "auto_send_email") == "1"
                for a in applicants_data:
                    ex = db.execute("SELECT id FROM applicants WHERE accio_order_number=?", (a["accio_order_number"],)).fetchone() if a["accio_order_number"] else None
                    if not ex:
                        db.execute("INSERT INTO applicants (first_name,last_name,email,phone,accio_order_number,accio_remote_number) VALUES (?,?,?,?,?,?)",
                                   (a["first_name"],a["last_name"],a["email"],a["phone"],a["accio_order_number"],a["accio_remote_number"]))
                        new_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
                        added += 1
                        # Auto-assign a payment code if enabled
                        if auto_assign:
                            code_row = db.execute("SELECT id, code FROM codes WHERE assigned_to IS NULL LIMIT 1").fetchone()
                            if code_row:
                                db.execute("UPDATE codes SET assigned_to=?, assigned_date=datetime('now') WHERE id=?", (new_id, code_row["id"]))
                                db.execute("UPDATE applicants SET assigned_code=? WHERE id=?", (code_row["code"], new_id))
                                db.commit()
                                # Auto-send email if enabled
                                if auto_email and a["email"]:
                                    try:
                                        send_release_email(db, new_id)
                                    except Exception:
                                        pass
                db.execute("UPDATE xml_log SET parsed_status='success' WHERE id=(SELECT MAX(id) FROM xml_log)")
                db.commit()
                # Respond with Accio-compatible XML (no <error> node = accepted)
                self._send(200, '<?xml version="1.0" encoding="UTF-8"?>\n<XML></XML>', "text/xml")
                return

            # API: Bulk code upload (JSON)
            if path == "/api/codes":
                length = int(self.headers.get("Content-Length", 0))
                raw = self.rfile.read(length).decode()
                try:
                    data = json.loads(raw)
                    codes_list = data.get("codes", [])
                    batch_name = data.get("batch_name", f"API Import {datetime.now().strftime('%Y-%m-%d %H:%M')}")
                    if not codes_list:
                        self._send(400, json.dumps({"status": "error", "message": "No codes provided. Send {\"codes\": [\"CODE1\", \"CODE2\", ...]}"}), "application/json")
                        return
                    imported, dups = 0, 0
                    db.execute("BEGIN TRANSACTION")
                    for code in codes_list:
                        code = str(code).strip()
                        if code:
                            try:
                                db.execute("INSERT INTO codes (code, batch_name) VALUES (?, ?)", (code, batch_name))
                                imported += 1
                            except sqlite3.IntegrityError:
                                dups += 1
                    db.execute("COMMIT")
                    self._send(200, json.dumps({"status": "success", "imported": imported, "duplicates": dups, "batch": batch_name}), "application/json")
                except json.JSONDecodeError:
                    self._send(400, json.dumps({"status": "error", "message": "Invalid JSON"}), "application/json")
                except Exception as e:
                    db.execute("ROLLBACK")
                    self._send(500, json.dumps({"status": "error", "message": str(e)}), "application/json")
                return

            # API: Bulk code upload from file via multipart
            if path == "/api/codes/upload":
                form_data = self._parse_form()
                if isinstance(form_data, cgi.FieldStorage) and "file" in form_data:
                    fi = form_data["file"]
                    col = int(form_data.getfirst("column", "0"))
                    batch = form_data.getfirst("batch_name", f"API Upload {datetime.now().strftime('%Y-%m-%d %H:%M')}")
                    fname = fi.filename or "upload.xlsx"
                    fpath = os.path.join(UPLOAD_FOLDER, f"api_{datetime.now().strftime('%Y%m%d%H%M%S')}_{fname}")
                    with open(fpath, "wb") as f:
                        f.write(fi.file.read())
                    try:
                        imported, dups, err = import_codes_from_file(fpath, column_index=col, skip_header=True, batch_name=batch)
                        if err:
                            self._send(400, json.dumps({"status": "error", "message": err}), "application/json")
                        else:
                            self._send(200, json.dumps({"status": "success", "imported": imported, "duplicates": dups, "batch": batch}), "application/json")
                    finally:
                        if os.path.exists(fpath): os.remove(fpath)
                else:
                    self._send(400, json.dumps({"status": "error", "message": "Send file as multipart form with field name 'file'"}), "application/json")
                return

            # Form submissions
            form_data = self._parse_form()

            def fv(name, default=""):
                """Get form value from either FieldStorage or dict."""
                if isinstance(form_data, cgi.FieldStorage):
                    item = form_data.getfirst(name, default)
                    return item if isinstance(item, str) else item.decode() if item else default
                else:
                    vals = form_data.get(name, [default])
                    return vals[0] if vals else default

            if path == "/applicants/add":
                db.execute("INSERT INTO applicants (first_name,last_name,email,phone,accio_order_number,notes) VALUES (?,?,?,?,?,?)",
                           (fv("first_name"), fv("last_name"), fv("email"), fv("phone"), fv("accio_order_number"), fv("notes")))
                db.commit()
                flash("Applicant added.", "success")
                self._redirect("/applicants")

            elif path.startswith("/applicants/") and path.endswith("/assign-code"):
                aid = int(path.split("/")[2])
                code_val, msg = assign_code(db, aid)
                flash(f"Code {code_val} assigned." if code_val else f"Failed: {msg}", "success" if code_val else "error")
                self._redirect("/applicants")

            elif path.startswith("/applicants/") and path.endswith("/send-email"):
                aid = int(path.split("/")[2])
                ok, msg = send_release_email(db, aid)
                flash(msg, "success" if ok else "error")
                self._redirect("/applicants")

            elif path.startswith("/applicants/") and path.endswith("/assign-and-send"):
                aid = int(path.split("/")[2])
                a = db.execute("SELECT * FROM applicants WHERE id=?", (aid,)).fetchone()
                if a and not a["assigned_code"]:
                    assign_code(db, aid)
                ok, msg = send_release_email(db, aid)
                flash("Code assigned & email sent!" if ok else f"Code assigned but email failed: {msg}", "success" if ok else "error")
                self._redirect("/applicants")

            elif path == "/applicants/bulk-process":
                pending = db.execute("SELECT * FROM applicants WHERE status='pending' AND email IS NOT NULL AND email!=''").fetchall()
                succ, fail = 0, 0
                for a in pending:
                    c_val, _ = assign_code(db, a["id"])
                    if c_val:
                        ok, _ = send_release_email(db, a["id"])
                        if ok: succ += 1
                        else: fail += 1
                    else:
                        fail += 1; break
                flash(f"Done: {succ} sent, {fail} failed.", "success")
                self._redirect("/applicants")

            elif path == "/codes/add-manual":
                codes_text = fv("codes")
                batch = fv("batch_name", "Manual")
                imp, dup = 0, 0
                for line in codes_text.strip().split("\n"):
                    code_str = line.strip()
                    if code_str:
                        try:
                            db.execute("INSERT INTO codes (code,batch_name) VALUES (?,?)", (code_str, batch))
                            imp += 1
                        except sqlite3.IntegrityError:
                            dup += 1
                db.commit()
                flash(f"Added {imp} codes ({dup} duplicates skipped).", "success")
                self._redirect("/codes")

            elif path == "/codes/import":
                if isinstance(form_data, cgi.FieldStorage):
                    file_item = form_data["file"]
                    col_idx = int(form_data.getfirst("column_index", "0"))
                    skip_h = form_data.getfirst("skip_header") == "on"
                    batch = form_data.getfirst("batch_name", "Import")
                    fname = file_item.filename

                    fpath = os.path.join(UPLOAD_FOLDER, f"import_{datetime.now().strftime('%Y%m%d%H%M%S')}_{fname}")
                    with open(fpath, "wb") as f:
                        f.write(file_item.file.read())

                    # Use the optimized bulk import engine
                    imp, dup, err = import_codes_from_file(fpath, column_index=col_idx, skip_header=skip_h, batch_name=batch)
                    if err:
                        flash(f"Import error: {err}", "error")
                    else:
                        flash(f"Imported {imp} codes ({dup} duplicates) from '{batch}'.", "success")
                    if os.path.exists(fpath): os.remove(fpath)
                self._redirect("/codes")

            elif path == "/settings":
                if isinstance(form_data, cgi.FieldStorage):
                    for key in DEFAULT_SETTINGS:
                        val = form_data.getfirst(key)
                        if val is not None:
                            set_setting(db, key, val)
                    # Handle file upload
                    if "release_form_file" in form_data:
                        fi = form_data["release_form_file"]
                        if fi.filename:
                            dest = os.path.join(UPLOAD_FOLDER, "release_form.pdf")
                            with open(dest, "wb") as f:
                                f.write(fi.file.read())
                            set_setting(db, "release_form_path", dest)
                else:
                    for key in DEFAULT_SETTINGS:
                        vals = form_data.get(key)
                        if vals:
                            set_setting(db, key, vals[0])
                flash("Settings saved.", "success")
                self._redirect("/settings")

            elif path == "/settings/test-email":
                addr = fv("test_email") or get_setting(db, "sender_email")
                if not addr:
                    flash("No email address.", "error")
                else:
                    try:
                        srv_host = get_setting(db, "smtp_server")
                        srv_port = int(get_setting(db, "smtp_port") or 587)
                        srv_user = get_setting(db, "smtp_username")
                        srv_pass = get_setting(db, "smtp_password")
                        use_tls = get_setting(db, "smtp_use_tls") == "1"
                        sender = get_setting(db, "sender_email")
                        sname = get_setting(db, "sender_name")
                        msg = MIMEText("Test email from Fingerprint Release Manager. SMTP is working!")
                        msg["From"] = f"{sname} <{sender}>"
                        msg["To"] = addr
                        msg["Subject"] = "Test - Fingerprint Release Manager"
                        srv = smtplib.SMTP(srv_host, srv_port)
                        if use_tls: srv.starttls()
                        if srv_user and srv_pass: srv.login(srv_user, srv_pass)
                        srv.send_message(msg)
                        srv.quit()
                        flash(f"Test email sent to {addr}!", "success")
                    except Exception as e:
                        flash(f"Test failed: {e}", "error")
                self._redirect("/settings")
            else:
                self._send(404, "Not found")
        finally:
            db.close()

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    init_db()
    start_folder_watcher()
    print(f"""
    ╔══════════════════════════════════════════════════════════╗
    ║   Fingerprint Release Manager                            ║
    ║   Web UI:      http://localhost:{PORT}                     ║
    ║                                                          ║
    ║   AUTO-IMPORT:                                           ║
    ║   Drop .xlsx/.csv files into the 'watch/' folder         ║
    ║   and codes will be imported automatically!              ║
    ║   Watch folder: {WATCH_FOLDER:<40s}  ║
    ║                                                          ║
    ║   API ENDPOINTS:                                         ║
    ║   POST /api/accio-push     (Accio Data XML results)      ║
    ║   POST /api/codes          (JSON: {{"codes":["..."]}} )    ║
    ║   POST /api/codes/upload   (Multipart file upload)       ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    server = HTTPServer((HOST, PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        _watcher_running = False
        print("\nShutting down...")
        server.server_close()
