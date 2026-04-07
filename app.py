"""
Fingerprint Release Manager - v3.0 (SMS + Email)
Integrates with Accio Data XML API to automate fingerprint release form distribution.

New Features in v3.0:
- SMS notifications via Twilio (texts applicants alongside email)
- Dual-channel delivery: email + SMS sent automatically on code assignment
- SMS logging and tracking (sms_log table)
- Twilio configuration in Settings UI with test SMS
- Phone number normalization for reliable SMS delivery

Existing Features:
- Multi-user login system with session management
- Client tracking from Accio XML
- Dashboard with analytics
- Email open tracking with pixels
- Applicant status workflow
- Clients management page

Workflow:
1. Receives applicant data via XML push from Accio Data (or manual entry)
2. Manages a pool of IdentoGO one-time payment codes (imported from Excel)
3. Assigns a code to each applicant
4. Emails the applicant their fingerprint release form PDF with their assigned code
5. Sends SMS to applicant with code, instructions, and scheduling info
6. Tracks email opens, SMS delivery, and applicant status

Built with Python standard library + openpyxl for Excel support + twilio for SMS.
"""

import os
import sys
import json
import smtplib
import logging
import urllib.parse
import urllib.request
import cgi
import io
import csv
import shutil
import base64
import uuid
import hashlib
try:
    import bcrypt
    HAS_BCRYPT = True
except ImportError:
    HAS_BCRYPT = False
    print("WARNING: bcrypt not installed. Run: pip install bcrypt  (required for Accio §1.7 compliance)")
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from http.server import HTTPServer, BaseHTTPRequestHandler
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.utils import formatdate, make_msgid
from xml.etree import ElementTree as ET
from string import Template
import time
import re

try:
    import psycopg2
    import psycopg2.extras
    HAS_PG = True
except ImportError:
    HAS_PG = False
    print("WARNING: psycopg2 not installed. Install with: pip install psycopg2-binary")

try:
    import openpyxl
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False
    print("WARNING: openpyxl not installed. Excel import will be limited to CSV only.")
    print("Install with: pip install openpyxl")

try:
    from twilio.rest import Client as TwilioClient
    HAS_TWILIO = True
except ImportError:
    HAS_TWILIO = False
    print("WARNING: twilio not installed. SMS notifications will be disabled.")
    print("Install with: pip install twilio")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_URL = os.environ.get("DATABASE_URL", "")
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
WATCH_FOLDER = os.path.join(BASE_DIR, "watch")
PROCESSED_FOLDER = os.path.join(BASE_DIR, "watch", "processed")
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")
HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 5000))
# Max body size for inbound XML (10 MB) — prevents memory exhaustion DoS
MAX_XML_BODY = 10 * 1024 * 1024

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(WATCH_FOLDER, exist_ok=True)
os.makedirs(PROCESSED_FOLDER, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
class DBHelper:
    """Wrapper to provide sqlite3-like interface over psycopg2."""
    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=None):
        cur = self._conn.cursor()
        cur.execute(sql, params or ())
        return cur

    def commit(self):
        pass  # autocommit is on

    def rollback(self):
        # FIX: Added missing rollback() method. With autocommit=True each statement
        # is its own transaction, so this is a no-op, but required after
        # psycopg2.IntegrityError to keep the connection in a clean state.
        try:
            self._conn.rollback()
        except Exception:
            pass

    def close(self):
        self._conn.close()


def get_db():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    conn.autocommit = True
    return DBHelper(conn)


def init_db():
    db = get_db()
    db.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            display_name TEXT,
            role TEXT DEFAULT 'user',
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW(),
            last_login TIMESTAMP
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            created_at TIMESTAMP DEFAULT NOW(),
            expires_at TIMESTAMP
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            id SERIAL PRIMARY KEY,
            company_name TEXT NOT NULL,
            account_name TEXT,
            contact_email TEXT,
            contact_phone TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            applicant_count INTEGER DEFAULT 0
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS applicants (
            id SERIAL PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT,
            phone TEXT,
            accio_order_number TEXT,
            accio_remote_number TEXT,
            status TEXT DEFAULT 'pending',
            assigned_code TEXT,
            email_sent BOOLEAN DEFAULT FALSE,
            email_sent_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            notes TEXT,
            client_id INTEGER REFERENCES clients(id),
            date_of_birth TEXT,
            last_four_ssn VARCHAR(4)
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS codes (
            id SERIAL PRIMARY KEY,
            code TEXT NOT NULL UNIQUE,
            status TEXT DEFAULT 'available',
            assigned_to INTEGER REFERENCES applicants(id),
            assigned_at TIMESTAMP,
            assigned_date TIMESTAMP,
            imported_at TIMESTAMP DEFAULT NOW(),
            batch_name TEXT
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS email_log (
            id SERIAL PRIMARY KEY,
            applicant_id INTEGER REFERENCES applicants(id),
            recipient_email TEXT,
            subject TEXT,
            status TEXT,
            error_message TEXT,
            sent_at TIMESTAMP DEFAULT NOW()
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS email_tracking (
            id SERIAL PRIMARY KEY,
            applicant_id INTEGER REFERENCES applicants(id),
            email_log_id INTEGER REFERENCES email_log(id),
            tracking_token TEXT UNIQUE NOT NULL,
            opened_at TIMESTAMP,
            first_human_open_at TIMESTAMP,
            open_count INTEGER DEFAULT 0,
            bot_open_count INTEGER DEFAULT 0,
            is_bot_open BOOLEAN DEFAULT FALSE,
            user_agent TEXT
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS xml_log (
            id SERIAL PRIMARY KEY,
            direction TEXT,
            raw_xml TEXT,
            parsed_status TEXT,
            error_message TEXT,
            received_at TIMESTAMP DEFAULT NOW()
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS password_reset_tokens (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
            token TEXT UNIQUE NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            used BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # SMS log table — tracks all outbound text messages
    db.execute("""
        CREATE TABLE IF NOT EXISTS sms_log (
            id SERIAL PRIMARY KEY,
            applicant_id INTEGER REFERENCES applicants(id),
            recipient_phone TEXT,
            message_body TEXT,
            twilio_sid TEXT,
            status TEXT,
            error_message TEXT,
            sent_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # Add new columns to existing tables if they don't exist
    try:
        db.execute("ALTER TABLE users ADD COLUMN recovery_email TEXT")
    except psycopg2.Error:
        pass  # Column already exists
    try:
        db.execute("ALTER TABLE applicants ADD COLUMN client_id INTEGER REFERENCES clients(id)")
    except psycopg2.Error:
        pass  # Column already exists

    # New columns to capture Accio sub-order number and order type for postback
    try:
        db.execute("ALTER TABLE applicants ADD COLUMN accio_sub_order TEXT")
    except psycopg2.Error:
        pass  # Column already exists
    try:
        db.execute("ALTER TABLE applicants ADD COLUMN accio_order_type TEXT")
    except psycopg2.Error:
        pass  # Column already exists

    # LAPS integration columns: DOB for display/matching, last 4 SSN for matching
    try:
        db.execute("ALTER TABLE applicants ADD COLUMN date_of_birth TEXT")
    except psycopg2.Error:
        pass  # Column already exists
    try:
        db.execute("ALTER TABLE applicants ADD COLUMN last_four_ssn VARCHAR(4)")
    except psycopg2.Error:
        pass  # Column already exists

    # Migration: add SMS tracking columns to applicants for existing databases
    for col_sql in [
        "ALTER TABLE applicants ADD COLUMN sms_sent BOOLEAN DEFAULT FALSE",
        "ALTER TABLE applicants ADD COLUMN sms_sent_at TIMESTAMP",
    ]:
        try:
            db.execute(col_sql)
        except psycopg2.Error:
            pass  # Column already exists

    # Migration: add bot-detection columns to email_tracking for existing databases
    for col_sql in [
        "ALTER TABLE email_tracking ADD COLUMN first_human_open_at TIMESTAMP",
        "ALTER TABLE email_tracking ADD COLUMN bot_open_count INTEGER DEFAULT 0",
        "ALTER TABLE email_tracking ADD COLUMN is_bot_open BOOLEAN DEFAULT FALSE",
    ]:
        try:
            db.execute(col_sql)
        except psycopg2.Error:
            pass  # Column already exists

    db.close()

    # Ensure the canonical admin account exists with bcrypt credentials.
    # This runs on every startup so it handles three cases:
    #   1. Fresh database — creates admin from scratch (requires env vars)
    #   2. Old database with legacy 'admin' account — renames and re-hashes
    #   3. Admin already exists — updates password hash to bcrypt if needed
    db = get_db()
    _default_user = os.environ.get("DEFAULT_ADMIN_USER", "").strip()
    _default_pass = os.environ.get("DEFAULT_ADMIN_PASSWORD", "").strip()
    if not _default_user or not _default_pass:
        # Only warn on first run when no admin exists yet; otherwise the
        # admin account is already in the database and env vars aren't needed.
        existing_check = db.execute("SELECT id FROM users WHERE role='admin' LIMIT 1").fetchone()
        if existing_check is None:
            logger.critical(
                "DEFAULT_ADMIN_USER and DEFAULT_ADMIN_PASSWORD environment variables "
                "are required for initial setup. Set them in your Render/hosting "
                "environment variables, then restart."
            )
            print("\n*** FATAL: Set DEFAULT_ADMIN_USER and DEFAULT_ADMIN_PASSWORD "
                  "environment variables before first run. ***\n")
            db.close()
            sys.exit(1)
        else:
            # Admin already exists in DB — no env vars needed for normal startup
            db.close()
            return
    if HAS_BCRYPT:
        pw_hash = bcrypt.hashpw(_default_pass.encode(), bcrypt.gensalt(rounds=12)).decode()
    else:
        salt = os.urandom(32)
        pw_hash = "sha256:" + hashlib.sha256(_default_pass.encode() + salt).hexdigest() + "$" + base64.b64encode(salt).decode()

    existing = db.execute("SELECT id, username FROM users WHERE role='admin' ORDER BY id LIMIT 1").fetchone()
    if existing is None:
        # No admin at all — create fresh
        db.execute(
            "INSERT INTO users (username, password_hash, display_name, role, is_active) VALUES (%s, %s, %s, %s, %s)",
            (_default_user, pw_hash, "Administrator", "admin", True)
        )
        logger.info("Created admin user '%s'", _default_user)
    else:
        # Admin exists — rename to correct username and re-hash password with bcrypt
        db.execute(
            "UPDATE users SET username=%s, password_hash=%s, is_active=TRUE WHERE id=%s",
            (_default_user, pw_hash, existing["id"])
        )
        logger.info("Migrated admin account to username '%s' with bcrypt hash", _default_user)
    db.close()


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
DEFAULT_SETTINGS = {
    "accio_account": os.environ.get("ACCIO_ACCOUNT", ""),
    "accio_username": os.environ.get("ACCIO_USERNAME", ""),
    "accio_password": os.environ.get("ACCIO_PASSWORD", ""),
    "accio_post_url": os.environ.get("ACCIO_POST_URL", "https://fingerprint-release-manager1.onrender.com/api/accio-push"),
    "accio_researcher_url": os.environ.get("ACCIO_RESEARCHER_URL", ""),
    "smtp_server": os.environ.get("SMTP_HOST", "smtp.gmail.com"),
    "smtp_port": os.environ.get("SMTP_PORT", "587"),
    "smtp_username": os.environ.get("SMTP_USER", ""),
    "smtp_password": os.environ.get("SMTP_PASS", ""),
    "smtp_use_tls": os.environ.get("SMTP_USE_TLS", "1"),
    "sender_email": os.environ.get("SENDER_EMAIL", ""),
    "sender_name": os.environ.get("SENDER_NAME", "Fingerprints Required"),
    "email_subject": "Fingerprint Processing - Next Steps from {company_name}",
    "email_body": """Hello {first_name} {last_name},

Thank you for working with {company_name}. Please see the attached form regarding your upcoming fingerprint appointment.

Your IdentoGO reference code: {code}

Please bring this code to your IdentoGO appointment. The processing fee has already been covered on your behalf -- simply present the code above when you arrive.

Steps to complete:
1. Review the attached Fingerprint Release Form
2. Visit your assigned IdentoGO location
3. Present your reference code: {code}
4. Complete the fingerprinting process

If you have any questions, feel free to reach out to us directly.

Best regards,
{company_name}""",
    "release_form_path": "",
    "company_name": "Background Research Solutions, LLC",
    "ori_number": "",
    "auto_assign_codes": "1",
    "auto_send_email": "1",
    # --- SMS / Twilio Settings ---
    "twilio_account_sid": os.environ.get("TWILIO_ACCOUNT_SID", ""),
    "twilio_auth_token": os.environ.get("TWILIO_AUTH_TOKEN", ""),
    "twilio_from_number": os.environ.get("TWILIO_FROM_NUMBER", ""),
    "auto_send_sms": "1",
    "sms_body": """Hello {first_name} {last_name}, this is {company_name}. Your fingerprint appointment is ready!

Your IdentoGO code: {code}

Steps:
1. Visit your assigned IdentoGO location
2. Present code: {code}
3. The processing fee is already covered

Questions? Reply to this text or contact us directly.

- {company_name}""",
}


def get_setting(db, key):
    cur = db.execute("SELECT value FROM settings WHERE key = %s", (key,))
    row = cur.fetchone()
    if row:
        return row["value"]
    return DEFAULT_SETTINGS.get(key, "")


def set_setting(db, key, value):
    db.execute(
        "INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        (key, value)
    )


# ---------------------------------------------------------------------------
# Authentication & Session Management
# ---------------------------------------------------------------------------
def hash_password(password):
    """Hash a password using bcrypt (Accio Security Policy §1.7 compliant).

    bcrypt is an adaptive one-way algorithm.  Unlike SHA-256, its work factor
    (cost) is configurable and can be increased over time, and no efficient
    cracking algorithm exists, satisfying §1.7's requirement for 'a one-way
    hashing algorithm for which no known algorithm for cracking in a useful
    timeframe is known'.
    """
    if HAS_BCRYPT:
        return bcrypt.hashpw(password.encode(), bcrypt.gensalt(rounds=12)).decode()
    # Fallback: SHA-256 with random salt.  Install bcrypt for full compliance.
    salt = os.urandom(32)
    h = hashlib.sha256(password.encode() + salt).hexdigest()
    salt_b64 = base64.b64encode(salt).decode()
    return f"sha256:{h}${salt_b64}"


def verify_password(password, stored_hash):
    """Verify a password against a bcrypt or legacy SHA-256 hash.

    Supports three stored formats for seamless migration:
      1. bcrypt   — $2b$... prefix (current, §1.7 compliant)
      2. sha256:  — prefixed legacy format written by this app when bcrypt unavailable
      3. no-prefix SHA-256  — original format written by very early versions of the app
    """
    try:
        if stored_hash.startswith("$2b$") or stored_hash.startswith("$2a$"):
            # Native bcrypt hash
            if HAS_BCRYPT:
                return bcrypt.checkpw(password.encode(), stored_hash.encode())
            return False
        if stored_hash.startswith("sha256:"):
            # Prefixed legacy SHA-256 path
            _, rest = stored_hash.split(":", 1)
            h, salt_b64 = rest.split("$", 1)
            salt = base64.b64decode(salt_b64)
            computed = hashlib.sha256(password.encode() + salt).hexdigest()
            return computed == h
        # Original no-prefix SHA-256 format (64-char hex + salt)
        parts = stored_hash.split("$", 1)
        if len(parts) == 2 and len(parts[0]) == 64:
            h, salt_b64 = parts
            salt = base64.b64decode(salt_b64)
            computed = hashlib.sha256(password.encode() + salt).hexdigest()
            return computed == h
        return False
    except Exception:
        return False


def create_session(db, user_id):
    """Create a new session token for a user."""
    token = str(uuid.uuid4())
    expires_at = (datetime.now() + timedelta(hours=24)).isoformat()
    db.execute(
        "INSERT INTO sessions (token, user_id, expires_at) VALUES (%s, %s, %s)",
        (token, user_id, expires_at)
    )
    return token


def verify_session(db, token):
    """Verify a session token and return user data if valid."""
    if not token:
        return None
    cur = db.execute(
        "SELECT u.id, u.username, u.display_name, u.role FROM users u "
        "JOIN sessions s ON u.id = s.user_id "
        "WHERE s.token = %s AND s.expires_at > NOW() AND u.is_active = TRUE",
        (token,)
    )
    return cur.fetchone()


def delete_session(db, token):
    """Delete a session token from the database (logout)."""
    if token:
        db.execute("DELETE FROM sessions WHERE token = %s", (token,))


def get_session_from_cookie(cookie_header):
    """Extract session token from cookie header."""
    if not cookie_header:
        return None
    for part in cookie_header.split(";"):
        part = part.strip()
        if part.startswith("session_token="):
            return part.split("=", 1)[1]
    return None


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
        # Capture sub-order ID and type for Accio postback (Ch 9)
        sub_order_el = complete_order.find("subOrder")
        sub_order_id = (sub_order_el.get("id", "") or sub_order_el.get("number", "1")) if sub_order_el is not None else "1"
        order_type = (sub_order_el.get("type", "") or "Fingerprint") if sub_order_el is not None else "Fingerprint"
        for subject in complete_order.iter("subject"):
            first = _xt(subject, "name_first")
            last = _xt(subject, "name_last")
            email = _xt(subject, "email")
            phone = _xt(subject, "phone")
            dob = _extract_dob(subject)
            ssn4 = _extract_ssn_last4(subject)
            if first or last:
                applicants.append(dict(first_name=first, last_name=last, email=email,
                                       phone=phone, accio_order_number=order_number,
                                       accio_remote_number=remote_number, company_name="",
                                       accio_sub_order=sub_order_id, accio_order_type=order_type,
                                       date_of_birth=dob, last_four_ssn=ssn4))

    for po in root.iter("placeOrder"):
        company_name = ""
        ai = po.find("accountInfo")
        if ai is not None:
            company_name = _xt(ai, "company_name")
        account_name = ""
        ci = po.find("clientInfo")
        if ci is not None:
            account_name = _xt(ci, "account")

        oi = po.find("orderInfo")
        oi_email = ""
        oi_phone = ""
        if oi is not None:
            oi_email = _xt(oi, "requester_email")
            oi_phone = _xt(oi, "requester_phone")
        if not oi_email:
            if ci is not None:
                oi_email = _xt(ci, "primaryuser_contact_email")
                if not oi_phone:
                    oi_phone = _xt(ci, "primaryuser_contact_telephone")
        if not oi_email:
            if ai is not None:
                oi_email = _xt(ai, "primaryuser_contact_email")
                if not oi_phone:
                    oi_phone = _xt(ai, "primaryuser_contact_telephone")
        sub_order_el = po.find("subOrder")
        sub_order_id = (sub_order_el.get("id", "") or sub_order_el.get("number", "1")) if sub_order_el is not None else "1"
        order_type = (sub_order_el.get("type", "") or "Fingerprint") if sub_order_el is not None else "Fingerprint"
        for subject in po.iter("subject"):
            first = _xt(subject, "name_first")
            last = _xt(subject, "name_last")
            email = _xt(subject, "email") or oi_email
            phone = _xt(subject, "phone") or oi_phone
            dob = _extract_dob(subject)
            ssn4 = _extract_ssn_last4(subject)
            if first or last:
                applicants.append(dict(first_name=first, last_name=last, email=email,
                                       phone=phone, accio_order_number=po.get("number", ""),
                                       accio_remote_number="", company_name=company_name,
                                       account_name=account_name,
                                       accio_sub_order=sub_order_id, accio_order_type=order_type,
                                       date_of_birth=dob, last_four_ssn=ssn4))

    # --- Format 2: Action Letter XML Post (postLetter with orderInfo) ---
    for post_letter in root.iter("postLetter"):
        order_number = post_letter.get("remote_order", "") or post_letter.get("order", "")
        remote_order = post_letter.get("remote_order", "")
        sub_order_el = post_letter.find("subOrder")
        sub_order_id = (sub_order_el.get("id", "") or sub_order_el.get("number", "1")) if sub_order_el is not None else "1"
        order_type = (sub_order_el.get("type", "") or "Fingerprint") if sub_order_el is not None else "Fingerprint"
        order_info = post_letter.find("orderInfo")
        if order_info is not None:
            first = _xt(order_info, "name_first")
            last = _xt(order_info, "name_last")
            email = _xt(order_info, "email")
            phone = _xt(order_info, "phone_number") or _xt(order_info, "phone")
            dob = _extract_dob(order_info)
            ssn4 = _extract_ssn_last4(order_info)
            if not email:
                email = _xt(order_info, "requester_email")
            if not phone:
                phone = _xt(order_info, "requester_phone")
            if first or last:
                applicants.append(dict(first_name=first, last_name=last, email=email,
                                       phone=phone, accio_order_number=order_number,
                                       accio_remote_number=remote_order, company_name="",
                                       account_name="",
                                       accio_sub_order=sub_order_id, accio_order_type=order_type,
                                       date_of_birth=dob, last_four_ssn=ssn4))

    # --- Format 3: Vendor dispatch XML (orderRequest with subject) ---
    for order_req in root.iter("orderRequest"):
        order_number = order_req.get("order", "") or order_req.get("number", "")
        remote_number = order_req.get("remote_order", "")
        sub_order_el = order_req.find("subOrder")
        sub_order_id = (sub_order_el.get("id", "") or sub_order_el.get("number", "1")) if sub_order_el is not None else "1"
        order_type = (sub_order_el.get("type", "") or "Fingerprint") if sub_order_el is not None else "Fingerprint"
        for subject in order_req.iter("subject"):
            first = _xt(subject, "name_first") or _xt(subject, "firstName")
            last = _xt(subject, "name_last") or _xt(subject, "lastName")
            email = _xt(subject, "email") or _xt(subject, "InternetEmailAddress")
            phone = _xt(subject, "phone") or _xt(subject, "phone_number")
            dob = _extract_dob(subject)
            ssn4 = _extract_ssn_last4(subject)
            if first or last:
                applicants.append(dict(first_name=first, last_name=last, email=email,
                                       phone=phone, accio_order_number=order_number,
                                       accio_remote_number=remote_number, company_name="",
                                       account_name="",
                                       accio_sub_order=sub_order_id, accio_order_type=order_type,
                                       date_of_birth=dob, last_four_ssn=ssn4))

    # --- Format 4: Generic fallback - PersonalData or BackgroundSearchPackage ---
    if not applicants:
        for pd in root.iter("PersonalData"):
            pn = pd.find("PersonName")
            cm = pd.find("ContactMethod")
            first = last = email = phone = ""
            dob = _extract_dob(pd)
            ssn4 = _extract_ssn_last4(pd)
            if pn is not None:
                first = _xt(pn, "GivenName") or _xt(pn, "name_first")
                last = _xt(pn, "FamilyName") or _xt(pn, "name_last")
                if not dob:
                    dob = _extract_dob(pn)
            if cm is not None:
                email = _xt(cm, "InternetEmailAddress") or _xt(cm, "email")
                phone = _xt(cm, "FormattedNumber") or _xt(cm, "phone")
            if first or last:
                applicants.append(dict(first_name=first, last_name=last, email=email,
                                       phone=phone, accio_order_number="",
                                       accio_remote_number="", company_name="", account_name="",
                                       date_of_birth=dob, last_four_ssn=ssn4))

    # --- Format 5: AccioOrder XML (placeOrder > subject, email in orderInfo) ---
    if not applicants:
        for place_order in root.iter("placeOrder"):
            order_number = place_order.get("number", "")
            company_name = ""
            ai = place_order.find("accountInfo")
            if ai is not None:
                company_name = _xt(ai, "company_name")
            account_name = ""
            ci = place_order.find("clientInfo")
            if ci is not None:
                account_name = _xt(ci, "account")

            order_info = place_order.find("orderInfo")
            order_email = order_phone = ""
            if order_info is not None:
                order_email = (_xt(order_info, "requester_email") or
                               _xt(order_info, "requester_fax") or "")
                order_phone = _xt(order_info, "requester_phone") or ""
            if not order_email:
                if ci is not None:
                    order_email = _xt(ci, "primaryuser_contact_email")
                    if not order_phone:
                        order_phone = _xt(ci, "primaryuser_contact_telephone")
            if not order_email:
                if ai is not None:
                    order_email = _xt(ai, "primaryuser_contact_email")
                    if not order_phone:
                        order_phone = _xt(ai, "primaryuser_contact_telephone")
            sub_order_el = place_order.find("subOrder")
            sub_order_id = (sub_order_el.get("id", "") or sub_order_el.get("number", "1")) if sub_order_el is not None else "1"
            order_type = (sub_order_el.get("type", "") or "Fingerprint") if sub_order_el is not None else "Fingerprint"
            for subject in place_order.iter("subject"):
                first = _xt(subject, "name_first")
                last = _xt(subject, "name_last")
                email = (_xt(subject, "email") or _xt(subject, "Email") or
                         _xt(subject, "InternetEmailAddress") or _xt(subject, "email_address") or
                         _xt(subject, "EmailAddress") or _xt(subject, "e_mail") or
                         _xt(subject, "applicant_email") or _xt(subject, "candidate_email") or
                         _xt(subject, "contact_email"))
                phone = (_xt(subject, "phone") or _xt(subject, "Phone") or
                         _xt(subject, "phone_number") or _xt(subject, "PhoneNumber") or
                         _xt(subject, "FormattedNumber") or _xt(subject, "telephone") or
                         _xt(subject, "contact_phone") or _xt(subject, "home_phone") or
                         _xt(subject, "cell_phone") or _xt(subject, "mobile"))
                dob = _extract_dob(subject)
                ssn4 = _extract_ssn_last4(subject)
                if not email:
                    email = order_email
                if not phone:
                    phone = order_phone
                if first or last:
                    applicants.append(dict(first_name=first, last_name=last, email=email,
                                           phone=phone, accio_order_number=order_number,
                                           accio_remote_number="", company_name=company_name,
                                           account_name=account_name,
                                           accio_sub_order=sub_order_id, accio_order_type=order_type,
                                           date_of_birth=dob, last_four_ssn=ssn4))

    # --- Format 5b: <order> tag (older AccioOrder variants) ---
    if not applicants:
        for order in root.iter("order"):
            order_number = order.get("number", "")
            remote_number = order.get("remote_order", "")
            order_info = order.find("orderInfo")
            order_email = order_phone = ""
            if order_info is not None:
                order_email = _xt(order_info, "requester_email")
                order_phone = _xt(order_info, "requester_phone")
            sub_order_el = order.find("subOrder")
            sub_order_id = (sub_order_el.get("id", "") or sub_order_el.get("number", "1")) if sub_order_el is not None else "1"
            order_type = (sub_order_el.get("type", "") or "Fingerprint") if sub_order_el is not None else "Fingerprint"
            for subject in order.iter("subject"):
                first = _xt(subject, "name_first")
                last = _xt(subject, "name_last")
                email = (_xt(subject, "email") or _xt(subject, "Email") or
                         _xt(subject, "InternetEmailAddress") or _xt(subject, "email_address") or
                         _xt(subject, "contact_email"))
                phone = (_xt(subject, "phone") or _xt(subject, "Phone") or
                         _xt(subject, "phone_number") or _xt(subject, "FormattedNumber") or
                         _xt(subject, "contact_phone"))
                dob = _extract_dob(subject)
                ssn4 = _extract_ssn_last4(subject)
                if not email:
                    email = order_email
                if not phone:
                    phone = order_phone
                if first or last:
                    applicants.append(dict(first_name=first, last_name=last, email=email,
                                           phone=phone, accio_order_number=order_number,
                                           accio_remote_number=remote_number, company_name="",
                                           account_name="",
                                           accio_sub_order=sub_order_id, accio_order_type=order_type,
                                           date_of_birth=dob, last_four_ssn=ssn4))

    # --- Deep scan fallback ---
    if not applicants:
        def deep_scan_for_applicants(elem):
            found = []
            parent_map = {c: p for p in elem.iter() for c in p}
            for el in elem.iter():
                if el.tag == "name_first" and el.text:
                    first = el.text.strip()
                    parent = parent_map.get(el)
                    if parent is not None:
                        last = _xt(parent, "name_last")
                        email = phone = ""
                        for tag in ["email", "Email", "InternetEmailAddress", "email_address",
                                    "EmailAddress", "e_mail", "applicant_email", "candidate_email",
                                    "contact_email"]:
                            email = _xt(parent, tag)
                            if email:
                                break
                        for tag in ["phone", "Phone", "phone_number", "PhoneNumber",
                                    "FormattedNumber", "telephone", "contact_phone",
                                    "home_phone", "cell_phone", "mobile"]:
                            phone = _xt(parent, tag)
                            if phone:
                                break
                        dob = _extract_dob(parent) if parent is not None else ""
                        ssn4 = _extract_ssn_last4(parent) if parent is not None else ""
                        if first or last:
                            found.append(dict(first_name=first, last_name=last, email=email,
                                              phone=phone, accio_order_number="",
                                              accio_remote_number="", company_name="",
                                              account_name="",
                                              date_of_birth=dob, last_four_ssn=ssn4))
            return found
        applicants = deep_scan_for_applicants(root)

    return applicants, None


def _xt(el, tag):
    c = el.find(tag)
    return c.text.strip() if c is not None and c.text else ""


def _extract_dob(subject_el):
    """
    Extract date of birth from an XML subject element.
    Accio XML may use: dob, DOB, date_of_birth, DateOfBirth, birthDate, birth_date.
    Accio format is YYYYMMDD (e.g., 19850315) but we also handle YYYY-MM-DD and MM/DD/YYYY.
    Returns formatted string "MM/DD/YYYY" for display, or empty string.
    """
    import re as _re
    raw = ""
    for tag in ["dob", "DOB", "date_of_birth", "DateOfBirth", "birthDate", "birth_date",
                "BirthDate", "dateOfBirth"]:
        raw = _xt(subject_el, tag)
        if raw:
            break
    if not raw:
        return ""
    raw = raw.strip()
    # Format: YYYYMMDD (Accio standard)
    if _re.match(r"^\d{8}$", raw):
        return f"{raw[4:6]}/{raw[6:8]}/{raw[0:4]}"
    # Format: YYYY-MM-DD
    if _re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        parts = raw.split("-")
        return f"{parts[1]}/{parts[2]}/{parts[0]}"
    # Format: MM/DD/YYYY (already correct)
    if _re.match(r"^\d{2}/\d{2}/\d{4}$", raw):
        return raw
    return raw  # Return as-is if unrecognized format


def _extract_ssn_last4(subject_el):
    """
    Extract last 4 digits of SSN from an XML subject element.
    Accio XML SSN format is 9 digits, no dashes (e.g., 123456789).
    Also handles XXX-XX-XXXX format. Returns last 4 digits or empty string.
    SECURITY: Only the last 4 digits are stored. Full SSN is never persisted.
    """
    import re as _re
    raw = ""
    for tag in ["ssn", "SSN", "social_security", "SocialSecurityNumber",
                "social_security_number", "ssn_number"]:
        raw = _xt(subject_el, tag)
        if raw:
            break
    if not raw:
        return ""
    # Strip dashes and spaces
    digits_only = _re.sub(r"[^0-9]", "", raw)
    if len(digits_only) >= 4:
        return digits_only[-4:]
    return ""


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------
def send_release_email(db, applicant_id):
    a = db.execute("SELECT * FROM applicants WHERE id = %s", (applicant_id,)).fetchone()
    if not a:
        return False, "Applicant not found"
    if not a["email"]:
        return False, "No email address"
    if not a["assigned_code"]:
        return False, "No code assigned"

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

    msg = MIMEMultipart("mixed")
    msg["From"] = f"{sender_name} <{sender_email}>"
    msg["To"] = a["email"]
    msg["Subject"] = subj
    msg["Reply-To"] = sender_email
    # RFC 5322 required headers — missing these is a major spam signal
    msg["Message-ID"] = make_msgid(domain=sender_email.split("@")[-1] if "@" in sender_email else "localhost")
    msg["Date"] = formatdate(localtime=True)
    # List-Unsubscribe — required by Gmail/Yahoo for bulk senders since Feb 2024.
    # Points to Reply-To so recipients can request removal; prevents spam filtering.
    msg["List-Unsubscribe"] = f"<mailto:{sender_email}?subject=unsubscribe>"

    alt_part = MIMEMultipart("alternative")
    alt_part.attach(MIMEText(body, "plain"))

    company_name = get_setting(db, "company_name") or "BR Solutions"
    html_body = f"""<!DOCTYPE html>
<html lang="en" xmlns="http://www.w3.org/1999/xhtml">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
<body style="margin:0; padding:0; background-color:#f9f9f9;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#f9f9f9;">
<tr><td align="center" style="padding:20px 0;">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="background-color:#ffffff; border-radius:8px; overflow:hidden;">
<tr><td style="padding:30px 40px; font-family:Arial, Helvetica, sans-serif; font-size:15px; line-height:1.6; color:#333333;">
{h(body).replace(chr(10), '<br>')}
</td></tr>
<tr><td style="padding:15px 40px 25px; font-family:Arial, Helvetica, sans-serif; font-size:11px; color:#999999; border-top:1px solid #eeeeee;">
This message was sent by {h(company_name)}. If you believe you received this email in error, please contact us at {h(sender_email)}.
</td></tr>
</table>
</td></tr>
</table>
</body>
</html>"""

    alt_part.attach(MIMEText(html_body, "html"))
    msg.attach(alt_part)

    rfp = get_setting(db, "release_form_path")
    if rfp and os.path.exists(rfp):
        with open(rfp, "rb") as f:
            part = MIMEBase("application", "pdf")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", 'attachment; filename="Fingerprint_Release_Form.pdf"')
            msg.attach(part)

    try:
        logger.info(f"SMTP: Connecting to {smtp_server}:{smtp_port} (TLS={use_tls})")
        logger.info(f"SMTP: From={sender_email}, To={a['email']}, User={smtp_user}")
        srv = smtplib.SMTP(smtp_server, smtp_port, timeout=15)
        srv.set_debuglevel(0)  # Disabled — debuglevel=1 leaks SMTP credentials to logs
        if use_tls:
            srv.starttls()
            logger.info("SMTP: TLS established")
        if smtp_user and smtp_pass:
            srv.login(smtp_user, smtp_pass)
            logger.info("SMTP: Login successful")
        srv.send_message(msg)
        logger.info("SMTP: Message sent successfully")
        srv.quit()

        now = datetime.now().isoformat()
        db.execute(
            "INSERT INTO email_log (applicant_id,recipient_email,subject,status) VALUES (%s,%s,%s,'sent')",
            (applicant_id, a["email"], subj)
        )

        db.execute(
            "UPDATE applicants SET email_sent=TRUE, email_sent_at=%s, status='emailed', updated_at=%s WHERE id = %s",
            (now, now, applicant_id)
        )
        db.commit()

        # Post status note back to Accio (Ch 9 / Ch 11) — non-blocking: failure does
        # not abort the email success response.  Silently skipped if URL not configured.
        try:
            pb_ok, pb_msg = post_accio_result(db, applicant_id)
            if pb_ok:
                logger.info(f"Accio postback: {pb_msg}")
            else:
                logger.warning(f"Accio postback skipped/failed (non-fatal): {pb_msg}")
        except Exception as pb_exc:
            logger.error(f"Accio postback exception (non-fatal): {pb_exc}")

        return True, "Email sent"
    except Exception as e:
        logger.error(f"SMTP FAILED: {type(e).__name__}: {e}")
        db.execute(
            "INSERT INTO email_log (applicant_id,recipient_email,subject,status,error_message) VALUES (%s,%s,%s,'failed',%s)",
            (applicant_id, a["email"], subj, str(e))
        )
        # Update applicant status to 'email_failed' so the failure is visible
        # in the applicants list.  Preserve any previously-assigned code.
        now_fail = datetime.now().isoformat()
        db.execute(
            "UPDATE applicants SET status='email_failed', updated_at=%s WHERE id = %s",
            (now_fail, applicant_id)
        )
        db.commit()
        return False, str(e)


# ---------------------------------------------------------------------------
# SMS (Twilio)
# ---------------------------------------------------------------------------
def normalize_phone(raw_phone):
    """Normalize a US phone number to E.164 format (+1XXXXXXXXXX).

    Handles common formats from Accio XML:
      (225) 555-1234, 225-555-1234, 2255551234, +12255551234, 1-225-555-1234
    Returns None if the number can't be normalized to a valid 10-digit US number.
    """
    if not raw_phone:
        return None
    digits = re.sub(r"[^\d]", "", raw_phone)
    # Strip leading country code '1' if present (11 digits starting with 1)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return None
    return f"+1{digits}"


def send_release_sms(db, applicant_id):
    """Send an SMS to the applicant with their fingerprint release info via Twilio.

    Mirrors the email workflow: builds the message from the sms_body template,
    sends via Twilio REST API, logs to sms_log, and updates the applicant record.
    Returns (success: bool, message: str).
    """
    if not HAS_TWILIO:
        return False, "Twilio library not installed. Run: pip install twilio"

    a = db.execute("SELECT * FROM applicants WHERE id = %s", (applicant_id,)).fetchone()
    if not a:
        return False, "Applicant not found"
    if not a["phone"]:
        return False, "No phone number on file"
    if not a["assigned_code"]:
        return False, "No code assigned"

    # Normalize phone to E.164
    to_number = normalize_phone(a["phone"])
    if not to_number:
        return False, f"Invalid phone number: {a['phone']} (must be 10-digit US number)"

    # Load Twilio credentials
    account_sid = get_setting(db, "twilio_account_sid").strip()
    auth_token = get_setting(db, "twilio_auth_token").strip()
    from_number = get_setting(db, "twilio_from_number").strip()

    if not account_sid or not auth_token or not from_number:
        return False, "Twilio not configured. Go to Settings > SMS Configuration."

    # Build message body from template
    reps = dict(
        first_name=a["first_name"],
        last_name=a["last_name"],
        email=a.get("email", ""),
        code=a["assigned_code"],
        company_name=get_setting(db, "company_name"),
        ori_number=get_setting(db, "ori_number"),
    )
    try:
        body = get_setting(db, "sms_body").format(**reps)
    except (KeyError, ValueError) as fmt_err:
        return False, f"SMS template error: {fmt_err}"

    try:
        logger.info(f"SMS: Sending to {to_number} from {from_number}")
        client = TwilioClient(account_sid, auth_token)
        message = client.messages.create(
            body=body,
            from_=from_number,
            to=to_number
        )
        logger.info(f"SMS: Sent successfully, SID={message.sid}, Status={message.status}")

        now = datetime.now().isoformat()
        db.execute(
            "INSERT INTO sms_log (applicant_id,recipient_phone,message_body,twilio_sid,status) "
            "VALUES (%s,%s,%s,%s,%s)",
            (applicant_id, to_number, body, message.sid, message.status)
        )
        db.execute(
            "UPDATE applicants SET sms_sent=TRUE, sms_sent_at=%s, updated_at=%s WHERE id = %s",
            (now, now, applicant_id)
        )
        db.commit()
        return True, f"SMS sent (SID: {message.sid})"

    except Exception as e:
        logger.error(f"SMS FAILED: {type(e).__name__}: {e}")
        db.execute(
            "INSERT INTO sms_log (applicant_id,recipient_phone,message_body,status,error_message) "
            "VALUES (%s,%s,%s,'failed',%s)",
            (applicant_id, to_number, body, str(e))
        )
        db.commit()
        return False, str(e)


def post_accio_result(db, applicant_id):
    """Post a status note back to Accio's researcherxml endpoint.

    Implements Ch 9 (postResults block) and Ch 11 (notes_from_vendor_to_screeningfirm)
    of the Accio Data XML Integration Manual.  Sends filledStatus='filled' so Accio
    marks the order complete and moves it to the next queue, with the assigned code
    and a note visible to the screening firm.
    """
    a = db.execute("SELECT * FROM applicants WHERE id = %s", (applicant_id,)).fetchone()
    if not a:
        return False, "Applicant not found"

    researcher_url = get_setting(db, "accio_researcher_url").strip()
    if not researcher_url:
        # Silently skip — feature disabled when URL not configured
        return False, "Accio researcher URL not configured (skipped)"

    order_number = (a.get("accio_order_number") or "").strip()
    if not order_number:
        return False, "No Accio order number on record — postback skipped"

    accio_account  = get_setting(db, "accio_account")  or ""
    accio_username = get_setting(db, "accio_username") or ""
    accio_password = get_setting(db, "accio_password") or ""
    sub_order      = (a.get("accio_sub_order")  or "1").strip() or "1"
    order_type     = (a.get("accio_order_type") or "Fingerprint").strip() or "Fingerprint"
    assigned_code  = (a.get("assigned_code") or "").strip()

    sent_at = datetime.now(tz=ZoneInfo("America/Chicago")).strftime("%m/%d/%Y %I:%M %p CST")
    # Check if SMS was also sent for this applicant
    sms_status = ""
    if a.get("sms_sent"):
        sms_status = " SMS also sent."
    note_text = (
        f"FP Release email sent on {sent_at}. "
        f"Assigned fingerprint code: {assigned_code}.{sms_status}"
    )

    # Build postResults XML per Ch 9 / Ch 11 of Accio Data XML Integration Manual.
    # filledStatus='filled' — marks order complete, moves it to next queue in Accio.
    # filledCode='see comments' — "Search completed with additional comments" (Ch 9.4.1).
    # <notes_from_vendor_to_screeningfirm> — visible to screening firm, not end user (Ch 11).
    xml_body = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<ScreeningResults>\n'
        '  <mode>PROD</mode>\n'
        '  <login>\n'
        f'    <account>{h(accio_account)}</account>\n'
        f'    <username>{h(accio_username)}</username>\n'
        f'    <password>{h(accio_password)}</password>\n'
        '  </login>\n'
        f'  <postResults order=\'{h(order_number)}\' subOrder=\'{h(sub_order)}\'\n'
        f'               type=\'{h(order_type)}\'\n'
        "               filledStatus='filled'\n"
        "               filledCode='see comments'>\n"
        f'    <notes_from_vendor_to_screeningfirm>{h(note_text)}</notes_from_vendor_to_screeningfirm>\n'
        f'    <text>{h(note_text)}</text>\n'
        '  </postResults>\n'
        '</ScreeningResults>\n'
    )

    try:
        req = urllib.request.Request(
            researcher_url,
            data=xml_body.encode("utf-8"),
            headers={"Content-Type": "text/xml; charset=utf-8"},
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            response_body = resp.read().decode("utf-8", errors="replace")

        # Per Ch 9.2: no <error> node means success; presence of <error> is a failure
        if "<error" in response_body.lower():
            db.execute(
                "INSERT INTO xml_log (direction, raw_xml, parsed_status, error_message) VALUES (%s,%s,%s,%s)",
                ("postback", xml_body[:4000], "error", response_body[:500])
            )
            db.commit()
            logger.warning(f"Accio postback rejected for order {order_number}: {response_body[:200]}")
            return False, f"Accio rejected postback: {response_body[:200]}"

        db.execute(
            "INSERT INTO xml_log (direction, raw_xml, parsed_status) VALUES (%s,%s,%s)",
            ("postback", xml_body[:4000], "success")
        )
        db.commit()
        logger.info(f"Accio postback success for order {order_number} (code: {assigned_code})")
        return True, "Accio notified"

    except Exception as e:
        logger.error(f"Accio postback failed for order {order_number}: {type(e).__name__}: {e}")
        try:
            db.execute(
                "INSERT INTO xml_log (direction, raw_xml, parsed_status, error_message) VALUES (%s,%s,%s,%s)",
                ("postback", xml_body[:4000], "failed", str(e)[:500])
            )
            db.commit()
        except Exception:
            pass
        return False, str(e)


def assign_code(db, applicant_id):
    a = db.execute("SELECT * FROM applicants WHERE id = %s", (applicant_id,)).fetchone()
    if not a:
        return None, "Not found"
    if a["assigned_code"]:
        return a["assigned_code"], "Already assigned"
    # FIX: Added ORDER BY id for deterministic (FIFO) code assignment
    code_row = db.execute(
        "SELECT id, code FROM codes WHERE assigned_to IS NULL ORDER BY id LIMIT 1"
    ).fetchone()
    if not code_row:
        return None, "No codes available"
    now = datetime.now().isoformat()
    db.execute("UPDATE codes SET assigned_to=%s, assigned_date=%s WHERE id = %s",
               (applicant_id, now, code_row["id"]))
    db.execute("UPDATE applicants SET assigned_code=%s, status='code_assigned', updated_at=%s WHERE id = %s",
               (code_row["code"], now, applicant_id))
    db.commit()
    return code_row["code"], "OK"


def import_codes_from_file(filepath, column_index=0, skip_header=True, batch_name=None):
    """
    FIX: Replaced per-row DB connection (was opening/closing a new connection for every
    single code) with a single connection for the entire import. This prevents connection
    pool exhaustion on large imports and dramatically improves performance.
    """
    imported = 0
    duplicates = 0
    error_msg = None
    db = None
    try:
        db = get_db()
        if filepath.endswith(".xlsx") and HAS_OPENPYXL:
            wb = openpyxl.load_workbook(filepath)
            ws = wb.active
            start_row = 2 if skip_header else 1
            for row in ws.iter_rows(min_row=start_row):
                cell = row[column_index] if column_index < len(row) else None
                if cell and cell.value:
                    code = str(cell.value).strip()
                    if code:
                        try:
                            db.execute("INSERT INTO codes (code, batch_name) VALUES (%s, %s)",
                                       (code, batch_name or "Import"))
                            imported += 1
                        except psycopg2.IntegrityError:
                            db.rollback()
                            duplicates += 1
        else:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.reader(f)
                if skip_header:
                    next(reader, None)
                for row in reader:
                    if column_index < len(row):
                        code = row[column_index].strip()
                        if code:
                            try:
                                db.execute("INSERT INTO codes (code, batch_name) VALUES (%s, %s)",
                                           (code, batch_name or "Import"))
                                imported += 1
                            except psycopg2.IntegrityError:
                                db.rollback()
                                duplicates += 1
    except Exception as e:
        error_msg = str(e)
    finally:
        if db:
            db.close()
    return imported, duplicates, error_msg


def auto_detect_code_column(filepath):
    """Try to guess which column has the payment codes."""
    if filepath.endswith(".xlsx") and HAS_OPENPYXL:
        wb = openpyxl.load_workbook(filepath)
        ws = wb.active
        for col_idx, cell in enumerate(ws[1]):
            if cell.value and ("code" in str(cell.value).lower() or "payment" in str(cell.value).lower()):
                return col_idx
    else:
        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if header:
                    for col_idx, cell in enumerate(header):
                        if "code" in cell.lower() or "payment" in cell.lower():
                            return col_idx
        except Exception:
            pass
    return 0


_watcher_running = True


def start_folder_watcher():
    """Monitor watch folder and auto-import codes from dropped files."""
    import threading
    def watch():
        while _watcher_running:
            try:
                for fname in os.listdir(WATCH_FOLDER):
                    fpath = os.path.join(WATCH_FOLDER, fname)
                    if os.path.isfile(fpath) and (fname.endswith(".xlsx") or fname.endswith(".csv")):
                        logger.info(f"Auto-importing {fname}...")
                        col = auto_detect_code_column(fpath)
                        imp, dup, err = import_codes_from_file(
                            fpath, column_index=col, skip_header=True,
                            batch_name=f"Auto-Import {fname}"
                        )
                        if err:
                            logger.error(f"Import error: {err}")
                        else:
                            logger.info(f"Imported {imp} codes ({dup} dups) from {fname}")
                        dest = os.path.join(PROCESSED_FOLDER, fname)
                        shutil.move(fpath, dest)
            except Exception as e:
                logger.error(f"Watcher error: {e}")
            import time
            time.sleep(5)
    t = threading.Thread(target=watch, daemon=True)
    t.start()


# ---------------------------------------------------------------------------
# Flash Messages
# ---------------------------------------------------------------------------
# FIX: flash() was a complete no-op (just `pass`). Fixed to actually store messages.
# With the single-threaded HTTPServer, this module-level dict is safe between redirect cycles.
_flashes = {}


def flash(msg, cat="success"):
    """Store a flash message to be displayed on next page render."""
    _flashes[cat] = msg


def render_flashes():
    """Render and clear all pending flash messages."""
    global _flashes
    if not _flashes:
        return ""
    html = ""
    for cat, msg in list(_flashes.items()):
        icon = "check-circle" if cat == "success" else "exclamation-circle"
        html += f'<div class="alert alert-{h(cat)}"><i class="fas fa-{icon}"></i> {h(msg)}</div>'
    _flashes.clear()
    return html


# ---------------------------------------------------------------------------
# HTML Utilities
# ---------------------------------------------------------------------------
def h(text):
    """HTML escape."""
    return (str(text) if text is not None else "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;").replace("'", "&#39;")


def fmt_dt(val):
    """Format datetime for display."""
    if not val:
        return "-"
    if isinstance(val, str):
        try:
            val = datetime.fromisoformat(val)
        except Exception:
            return val
    return val.strftime("%Y-%m-%d %H:%M")


def render_page(title, content, active="", user=None):
    """Render a full page with navigation."""
    username_display = h(user["username"]) if user else ""
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{h(title)} - Fingerprint Release Manager</title>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
        <style>
            /* === FUTURISTIC DARK THEME — FP Release v3.0 === */
            :root {{
                --primary: #00d4ff;           /* Electric cyan accent */
                --primary-dark: #00a8cc;
                --primary-glow: rgba(0, 212, 255, 0.15);
                --accent-purple: #a855f7;     /* Neon purple secondary */
                --accent-teal: #2dd4bf;        /* Soft teal highlight */
                --success: #00e68a;
                --danger: #ff4d6a;
                --warning: #ffb020;
                --surface-0: #0a0e1a;         /* Deepest background */
                --surface-1: #111827;         /* Sidebar / panels */
                --surface-2: #1a2035;         /* Cards */
                --surface-3: #222b45;         /* Elevated surfaces */
                --glass: rgba(26, 32, 53, 0.65); /* Glassmorphism base */
                --glass-border: rgba(0, 212, 255, 0.12);
                --border: rgba(255,255,255,0.06);
                --border-hover: rgba(0, 212, 255, 0.25);
                --text-primary: #e8ecf4;
                --text-secondary: #8892a8;
                --text-muted: #5a6478;
                /* Legacy aliases for inline styles that reference old vars */
                --gray-50: var(--surface-3);
                --gray-100: var(--surface-2);
                --gray-200: var(--border);
                --gray-300: var(--text-muted);
                --gray-400: var(--text-secondary);
                --gray-500: var(--text-secondary);
                --gray-700: var(--text-secondary);
                --gray-900: var(--text-primary);
            }}
            * {{ margin: 0; padding: 0; box-sizing: border-box; }}
            body {{ font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: var(--surface-0); color: var(--text-primary); letter-spacing: 0.01em; }}

            /* --- Layout --- */
            .container {{ display: flex; min-height: 100vh; }}

            /* --- Sidebar: matte dark with subtle glow accents --- */
            .sidebar {{ width: 250px; background: linear-gradient(180deg, var(--surface-1) 0%, #0d1225 100%); color: white; padding: 2rem 0; overflow-y: auto; box-shadow: 2px 0 20px rgba(0,0,0,0.4); border-right: 1px solid var(--glass-border); position: relative; }}
            .sidebar::after {{ content: ''; position: absolute; top: 0; right: 0; width: 1px; height: 100%; background: linear-gradient(180deg, var(--primary) 0%, transparent 50%, var(--accent-purple) 100%); opacity: 0.3; }}
            .sidebar-brand {{ padding: 0 1.5rem 2rem; font-size: 1.5rem; font-weight: bold; display: flex; align-items: center; gap: 0.5rem; border-bottom: 1px solid rgba(255,255,255,0.06); }}
            .sidebar-brand i {{ color: var(--primary); filter: drop-shadow(0 0 6px rgba(0, 212, 255, 0.5)); }}
            .sidebar-brand span {{ background: linear-gradient(135deg, var(--primary), var(--accent-purple)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; }}
            .sidebar-nav {{ list-style: none; padding: 1rem 0; }}
            .sidebar-nav li {{ margin: 0; }}
            .sidebar-nav a {{ display: flex; align-items: center; gap: 0.75rem; padding: 0.75rem 1.5rem; color: var(--text-secondary); text-decoration: none; transition: all 0.3s ease; border-left: 3px solid transparent; }}
            .sidebar-nav a:hover {{ color: var(--primary); background: var(--primary-glow); padding-left: 1.75rem; }}
            .sidebar-nav a.active {{ color: var(--primary); background: var(--primary-glow); border-left: 3px solid var(--primary); padding-left: 1.5rem; box-shadow: inset 0 0 20px rgba(0,212,255,0.05); }}
            .sidebar-nav a.active i {{ filter: drop-shadow(0 0 4px rgba(0, 212, 255, 0.4)); }}

            /* --- Main content area --- */
            .main {{ flex: 1; display: flex; flex-direction: column; background: var(--surface-0); }}

            /* --- Top bar: frosted glass --- */
            .topbar {{ background: var(--glass); backdrop-filter: blur(20px); -webkit-backdrop-filter: blur(20px); padding: 1rem 2rem; border-bottom: 1px solid var(--glass-border); display: flex; justify-content: space-between; align-items: center; }}
            .topbar-user {{ display: flex; align-items: center; gap: 1rem; color: var(--text-secondary); }}
            .topbar-user a {{ color: var(--primary); text-decoration: none; font-size: 0.875rem; transition: all 0.2s; }}
            .topbar-user a:hover {{ color: var(--accent-teal); text-shadow: 0 0 8px rgba(0,212,255,0.3); }}

            .content {{ flex: 1; overflow-y: auto; padding: 2rem; }}
            .page-title {{ font-size: 2rem; font-weight: bold; margin-bottom: 1.5rem; color: var(--text-primary); letter-spacing: 0.02em; }}
            .page-title i {{ color: var(--primary); filter: drop-shadow(0 0 8px rgba(0,212,255,0.4)); }}

            /* --- Alerts with glow borders --- */
            .alert {{ padding: 1rem; border-radius: 0.75rem; margin-bottom: 1rem; display: flex; gap: 0.75rem; align-items: flex-start; backdrop-filter: blur(8px); }}
            .alert-success {{ background: rgba(0, 230, 138, 0.08); border: 1px solid rgba(0,230,138,0.3); color: var(--success); box-shadow: 0 0 15px rgba(0,230,138,0.05); }}
            .alert-error {{ background: rgba(255, 77, 106, 0.08); border: 1px solid rgba(255,77,106,0.3); color: var(--danger); box-shadow: 0 0 15px rgba(255,77,106,0.05); }}

            /* --- Cards: glassmorphism with 3D depth --- */
            .card {{ background: var(--glass); backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px); border: 1px solid var(--glass-border); border-radius: 0.75rem; box-shadow: 0 4px 24px rgba(0,0,0,0.3), 0 1px 0 rgba(255,255,255,0.03) inset; padding: 1.5rem; margin-bottom: 1.5rem; transition: border-color 0.3s ease, box-shadow 0.3s ease; }}
            .card:hover {{ border-color: var(--border-hover); box-shadow: 0 8px 32px rgba(0,0,0,0.4), 0 0 20px rgba(0,212,255,0.03); }}
            .card-title {{ font-size: 1.25rem; font-weight: 600; margin-bottom: 1rem; display: flex; align-items: center; gap: 0.5rem; color: var(--text-primary); }}
            .card-title i {{ color: var(--primary); filter: drop-shadow(0 0 4px rgba(0,212,255,0.3)); }}

            /* --- Stat cards: elevated with subtle inner glow --- */
            .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; margin-bottom: 2rem; }}
            .stat-card {{ background: linear-gradient(145deg, var(--surface-2), var(--surface-3)); border: 1px solid var(--glass-border); border-radius: 0.75rem; padding: 1.5rem; box-shadow: 0 4px 20px rgba(0,0,0,0.25), inset 0 1px 0 rgba(255,255,255,0.04); text-align: center; transition: transform 0.3s ease, box-shadow 0.3s ease; }}
            .stat-card:hover {{ transform: translateY(-2px); box-shadow: 0 8px 30px rgba(0,0,0,0.35), 0 0 20px rgba(0,212,255,0.06); }}
            .stat-value {{ font-size: 2rem; font-weight: bold; color: var(--primary); margin: 0.5rem 0; text-shadow: 0 0 20px rgba(0,212,255,0.2); }}
            .stat-label {{ color: var(--text-secondary); font-size: 0.875rem; text-transform: uppercase; letter-spacing: 0.05em; }}
            .stat-icon {{ font-size: 2rem; color: var(--primary); margin-bottom: 0.5rem; opacity: 0.7; filter: drop-shadow(0 0 6px rgba(0,212,255,0.3)); }}

            /* --- Tables: dark rows with cyan accent lines --- */
            table {{ width: 100%; border-collapse: collapse; margin-bottom: 1rem; }}
            thead {{ background: rgba(0, 212, 255, 0.04); border-bottom: 2px solid rgba(0,212,255,0.15); }}
            th {{ padding: 0.75rem; text-align: left; font-weight: 600; color: var(--text-secondary); font-size: 0.875rem; text-transform: uppercase; letter-spacing: 0.04em; }}
            td {{ padding: 0.75rem; border-bottom: 1px solid var(--border); color: var(--text-primary); }}
            tbody tr {{ transition: background 0.2s ease; }}
            tbody tr:hover {{ background: rgba(0, 212, 255, 0.04); }}

            /* --- Buttons: gradient fills with glow on hover --- */
            .btn {{ padding: 0.5rem 1rem; border: none; border-radius: 0.5rem; font-size: 0.875rem; font-weight: 500; cursor: pointer; text-decoration: none; display: inline-flex; align-items: center; gap: 0.5rem; transition: all 0.3s ease; }}
            .btn-primary {{ background: linear-gradient(135deg, var(--primary), #0099cc); color: #0a0e1a; font-weight: 600; }}
            .btn-primary:hover {{ box-shadow: 0 0 20px rgba(0,212,255,0.3); transform: translateY(-1px); }}
            .btn-success {{ background: linear-gradient(135deg, var(--success), #00b36b); color: #0a0e1a; font-weight: 600; }}
            .btn-success:hover {{ box-shadow: 0 0 20px rgba(0,230,138,0.3); transform: translateY(-1px); }}
            .btn-danger {{ background: linear-gradient(135deg, var(--danger), #cc2244); color: white; }}
            .btn-danger:hover {{ box-shadow: 0 0 20px rgba(255,77,106,0.3); transform: translateY(-1px); }}
            .btn-small {{ padding: 0.25rem 0.75rem; font-size: 0.75rem; }}

            /* --- Forms: dark inputs with glow focus --- */
            .form-group {{ margin-bottom: 1.5rem; }}
            label {{ display: block; margin-bottom: 0.5rem; font-weight: 500; color: var(--text-secondary); }}
            input[type="text"], input[type="email"], input[type="password"], input[type="number"], select, textarea {{ width: 100%; padding: 0.625rem 0.75rem; border: 1px solid rgba(255,255,255,0.08); border-radius: 0.5rem; font-size: 0.875rem; font-family: inherit; background: var(--surface-1); color: var(--text-primary); transition: all 0.3s ease; }}
            input:focus, select:focus, textarea:focus {{ outline: none; border-color: var(--primary); box-shadow: 0 0 0 3px rgba(0,212,255,0.1), 0 0 15px rgba(0,212,255,0.05); background: var(--surface-2); }}
            input::placeholder, textarea::placeholder {{ color: var(--text-muted); }}

            /* --- Status badges: glowing pill indicators --- */
            .status-badge {{ display: inline-block; padding: 0.25rem 0.75rem; border-radius: 1rem; font-size: 0.75rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.03em; }}
            .status-pending {{ background: rgba(255, 77, 106, 0.12); color: var(--danger); border: 1px solid rgba(255,77,106,0.2); }}
            .status-code_assigned {{ background: rgba(255, 176, 32, 0.12); color: var(--warning); border: 1px solid rgba(255,176,32,0.2); }}
            .status-emailed {{ background: rgba(0, 212, 255, 0.12); color: var(--primary); border: 1px solid rgba(0,212,255,0.2); }}
            .status-opened {{ background: rgba(0, 230, 138, 0.12); color: var(--success); border: 1px solid rgba(0,230,138,0.2); }}
            .status-completed {{ background: rgba(0, 230, 138, 0.12); color: var(--success); border: 1px solid rgba(0,230,138,0.2); }}
            .status-email_failed {{ background: rgba(255, 77, 106, 0.15); color: var(--danger); border: 1px solid rgba(255,77,106,0.3); animation: pulse-fail 2s ease-in-out infinite; }}
            @keyframes pulse-fail {{ 0%, 100% {{ opacity: 1; }} 50% {{ opacity: 0.6; }} }}

            /* --- Email status dots with glow --- */
            .email-status {{ display: inline-block; width: 12px; height: 12px; border-radius: 50%; margin-right: 0.25rem; }}
            .email-status-opened {{ background: var(--success); box-shadow: 0 0 6px rgba(0,230,138,0.4); }}
            .email-status-not-opened {{ background: var(--danger); box-shadow: 0 0 6px rgba(255,77,106,0.4); }}
            .email-status-unsent {{ background: var(--text-muted); }}

            .es {{ text-align: center; padding: 3rem; }}
            .es i {{ font-size: 4rem; color: var(--text-muted); margin-bottom: 1rem; }}
            .es h3 {{ color: var(--text-secondary); }}
            .grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem; }}

            /* --- Code elements: monospace with subtle highlight --- */
            code {{ background: rgba(0,212,255,0.06); border: 1px solid rgba(0,212,255,0.1); border-radius: 0.25rem; padding: 0.1rem 0.4rem; font-family: "SF Mono", "Fira Code", "Cascadia Code", monospace; color: var(--accent-teal); }}

            /* --- Links --- */
            a {{ color: var(--primary); transition: color 0.2s ease; }}
            a:hover {{ color: var(--accent-teal); }}

            /* --- Scrollbar: thin dark with cyan thumb --- */
            ::-webkit-scrollbar {{ width: 6px; }}
            ::-webkit-scrollbar-track {{ background: var(--surface-0); }}
            ::-webkit-scrollbar-thumb {{ background: rgba(0,212,255,0.2); border-radius: 3px; }}
            ::-webkit-scrollbar-thumb:hover {{ background: rgba(0,212,255,0.35); }}

            /* --- Subtle ambient glow animation on sidebar brand icon --- */
            @keyframes glow-pulse {{ 0%, 100% {{ filter: drop-shadow(0 0 6px rgba(0,212,255,0.5)); }} 50% {{ filter: drop-shadow(0 0 12px rgba(0,212,255,0.8)); }} }}
            .sidebar-brand i {{ animation: glow-pulse 3s ease-in-out infinite; }}

            /* --- Responsive --- */
            @media (max-width: 768px) {{
                .container {{ flex-direction: column; }}
                .sidebar {{ width: 100%; padding: 1rem 0; }}
                .sidebar-brand {{ padding: 1rem; }}
                .grid-2 {{ grid-template-columns: 1fr; }}
                .stats {{ grid-template-columns: 1fr; }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="sidebar">
                <div class="sidebar-brand">
                    <i class="fas fa-fingerprint"></i>
                    <span>FP Release</span>
                </div>
                <nav class="sidebar-nav">
                    <li><a href="/" class="{'active' if active == 'dashboard' else ''}"><i class="fas fa-chart-line"></i> Dashboard</a></li>
                    <li><a href="/applicants" class="{'active' if active == 'applicants' else ''}"><i class="fas fa-users"></i> Applicants</a></li>
                    <li><a href="/clients" class="{'active' if active == 'clients' else ''}"><i class="fas fa-building"></i> Clients</a></li>
                    <li><a href="/codes" class="{'active' if active == 'codes' else ''}"><i class="fas fa-barcode"></i> Codes</a></li>
                    <li><a href="/settings" class="{'active' if active == 'settings' else ''}"><i class="fas fa-cog"></i> Settings</a></li>
                    <li><a href="/logs" class="{'active' if active == 'logs' else ''}"><i class="fas fa-file-alt"></i> Logs</a></li>
                    {'<li><a href="/users" class="' + ("active" if active == "users" else "") + '"><i class="fas fa-users-cog"></i> Users</a></li>' if user and user.get("role") == "admin" else ""}
                    <li><a href="/profile" class="{'active' if active == 'profile' else ''}"><i class="fas fa-user-circle"></i> My Account</a></li>
                </nav>
            </div>
            <div class="main">
                <div class="topbar">
                    <div></div>
                    <div class="topbar-user">
                        <span><i class="fas fa-user"></i> {username_display}</span>
                        <a href="/logout">Logout</a>
                    </div>
                </div>
                <div class="content">
                    <h1 class="page-title"><i class="fas fa-fingerprint"></i> {h(title)}</h1>
                    {render_flashes()}
                    {content}
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    return html


def page_login(error=""):
    """Login page."""
    error_html = f'<div class="alert">{h(error)}</div>' if error else ""
    return f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Login - Fingerprint Release Manager</title>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
        <style>
            @keyframes glow-pulse {{ 0%, 100% {{ filter: drop-shadow(0 0 8px rgba(0, 212, 255, 0.6)); }} 50% {{ filter: drop-shadow(0 0 20px rgba(0, 212, 255, 0.9)); }} }}
            @keyframes float-in {{ from {{ opacity: 0; transform: translateY(20px); }} to {{ opacity: 1; transform: translateY(0); }} }}
            :root {{ --primary: #00d4ff; --accent-purple: #a855f7; --surface-0: #0a0e1a; --surface-1: rgba(15, 23, 42, 0.85); --text-primary: #e2e8f0; --text-muted: #64748b; }}
            * {{ margin: 0; padding: 0; box-sizing: border-box; }}
            body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: var(--surface-0); background-image: radial-gradient(ellipse at 20% 50%, rgba(0, 212, 255, 0.08) 0%, transparent 50%), radial-gradient(ellipse at 80% 50%, rgba(168, 85, 247, 0.06) 0%, transparent 50%); min-height: 100vh; display: flex; align-items: center; justify-content: center; }}
            .login-card {{ background: var(--surface-1); backdrop-filter: blur(16px); -webkit-backdrop-filter: blur(16px); border: 1px solid rgba(0, 212, 255, 0.15); border-radius: 1rem; box-shadow: 0 10px 40px rgba(0, 0, 0, 0.5), 0 0 60px rgba(0, 212, 255, 0.05); padding: 3rem; width: 100%; max-width: 420px; animation: float-in 0.6s ease-out; }}
            .login-brand {{ text-align: center; margin-bottom: 2rem; }}
            .login-brand i {{ font-size: 3rem; background: linear-gradient(135deg, var(--primary), var(--accent-purple)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin-bottom: 0.5rem; animation: glow-pulse 3s ease-in-out infinite; }}
            .login-brand h1 {{ font-size: 1.5rem; color: var(--text-primary); margin: 0; font-weight: 600; }}
            .login-brand p {{ color: var(--text-muted); margin: 0.5rem 0 0 0; font-size: 0.875rem; letter-spacing: 0.05em; }}
            .form-group {{ margin-bottom: 1.5rem; }}
            label {{ display: block; margin-bottom: 0.5rem; font-weight: 500; color: var(--text-primary); font-size: 0.875rem; }}
            input {{ width: 100%; padding: 0.75rem 1rem; border: 1px solid rgba(0, 212, 255, 0.2); border-radius: 0.5rem; font-size: 0.875rem; font-family: inherit; background: rgba(0, 0, 0, 0.3); color: var(--text-primary); transition: all 0.3s ease; }}
            input:focus {{ outline: none; border-color: var(--primary); box-shadow: 0 0 0 3px rgba(0, 212, 255, 0.15), 0 0 20px rgba(0, 212, 255, 0.1); }}
            input::placeholder {{ color: var(--text-muted); }}
            .btn {{ width: 100%; padding: 0.75rem; background: linear-gradient(135deg, var(--primary), var(--accent-purple)); color: white; border: none; border-radius: 0.5rem; font-size: 0.875rem; font-weight: 600; cursor: pointer; transition: all 0.3s ease; letter-spacing: 0.03em; }}
            .btn:hover {{ box-shadow: 0 0 25px rgba(0, 212, 255, 0.3), 0 0 50px rgba(168, 85, 247, 0.15); transform: translateY(-1px); }}
            .alert {{ background: rgba(239, 68, 68, 0.1); border: 1px solid rgba(239, 68, 68, 0.4); color: #f87171; padding: 0.75rem; border-radius: 0.5rem; margin-bottom: 1.5rem; font-size: 0.875rem; }}
        </style>
    </head>
    <body>
        <div class="login-card">
            <div class="login-brand">
                <i class="fas fa-fingerprint"></i>
                <h1>Fingerprint Release</h1>
                <p>Manager v3.0</p>
            </div>
            {error_html}
            <form method="POST" action="/login">
                <div class="form-group">
                    <label for="username">Username</label>
                    <input type="text" id="username" name="username" required autofocus autocomplete="username">
                </div>
                <div class="form-group">
                    <label for="password">Password</label>
                    <input type="password" id="password" name="password" required autocomplete="current-password">
                </div>
                <button type="submit" class="btn">Sign In</button>
            </form>
            <p style="text-align:center;margin-top:1.25rem;font-size:.875rem;">
                <a href="/forgot-password" style="color:var(--primary);text-decoration:none;transition:color 0.2s;">Forgot password?</a>
            </p>
        </div>
    </body>
    </html>
    """


def page_dashboard(db, nav_user=None):
    """Dashboard with analytics."""
    total_app = db.execute("SELECT COUNT(*) as cnt FROM applicants").fetchone()["cnt"]
    pending = db.execute("SELECT COUNT(*) as cnt FROM applicants WHERE status='pending'").fetchone()["cnt"]
    emailed = db.execute("SELECT COUNT(*) as cnt FROM applicants WHERE status='emailed'").fetchone()["cnt"]
    email_failed = db.execute("SELECT COUNT(*) as cnt FROM applicants WHERE status='email_failed'").fetchone()["cnt"]
    codes_avail = db.execute("SELECT COUNT(*) as cnt FROM codes WHERE assigned_to IS NULL").fetchone()["cnt"]
    codes_used = db.execute("SELECT COUNT(*) as cnt FROM codes WHERE assigned_to IS NOT NULL").fetchone()["cnt"]

    activity = db.execute("""
        SELECT type, id, col1, col2, ts FROM (
            SELECT 'new_applicant' as type, id, first_name as col1, last_name as col2, created_at as ts FROM applicants
            UNION ALL
            SELECT CASE WHEN status = 'failed' THEN 'email_failed' ELSE 'email_sent' END as type,
                   id, recipient_email as col1,
                   CASE WHEN status = 'failed' THEN COALESCE(error_message, 'Unknown error') ELSE subject END as col2,
                   sent_at as ts FROM email_log
        ) combined
        ORDER BY ts DESC NULLS LAST
        LIMIT 10
    """).fetchall()

    clients = db.execute("""
        SELECT c.id, c.company_name, c.account_name, COUNT(a.id) as app_count
        FROM clients c
        LEFT JOIN applicants a ON a.client_id = c.id
        GROUP BY c.id, c.company_name, c.account_name
        ORDER BY app_count DESC
        LIMIT 5
    """).fetchall()

    # Alert banner shown when there are applicants whose emails failed or were blocked
    fail_alert_html = ""
    if email_failed > 0:
        # Fetch the most recent failures for the alert details
        recent_fails = db.execute(
            "SELECT a.first_name, a.last_name, a.email, el.error_message, el.sent_at "
            "FROM applicants a "
            "JOIN email_log el ON el.applicant_id = a.id AND el.status = 'failed' "
            "WHERE a.status = 'email_failed' "
            "ORDER BY el.sent_at DESC LIMIT 5"
        ).fetchall()
        fail_details = ""
        for rf in recent_fails:
            err_short = h((rf["error_message"] or "Unknown error")[:120])
            fail_details += (
                f'<div style="padding:0.4rem 0; border-bottom:1px solid rgba(220,38,38,0.15);">'
                f'<strong>{h(rf["first_name"])} {h(rf["last_name"])}</strong> '
                f'&lt;{h(rf["email"] or "no email")}&gt; &mdash; '
                f'<span style="color:#991b1b;">{err_short}</span> '
                f'<span style="color:#6b7280; font-size:0.75rem;">({fmt_dt(rf["sent_at"])})</span>'
                f'</div>'
            )
        fail_alert_html = f"""
        <div style="background: rgba(220,38,38,0.08); border: 1px solid rgba(220,38,38,0.3); border-radius: 0.5rem; padding: 1rem 1.25rem; margin-bottom: 1.25rem; display: flex; align-items: flex-start; gap: 0.75rem;">
            <i class="fas fa-exclamation-triangle" style="color: #dc2626; font-size: 1.25rem; margin-top: 0.15rem;"></i>
            <div style="flex: 1;">
                <div style="font-weight: 600; color: #dc2626; margin-bottom: 0.25rem;">
                    {email_failed} applicant{'s' if email_failed != 1 else ''} with failed/blocked email{'s' if email_failed != 1 else ''}
                </div>
                <div style="font-size: 0.85rem; color: #374151; margin-bottom: 0.5rem;">
                    The following emails could not be delivered. Review and retry from the
                    <a href="/applicants" style="color: #dc2626; font-weight: 600;">Applicants</a> page.
                </div>
                {fail_details}
            </div>
        </div>
        """

    stats_html = f"""
    {fail_alert_html}
    <div class="stats">
        <div class="stat-card"><div class="stat-icon"><i class="fas fa-users"></i></div><div class="stat-label">Total Applicants</div><div class="stat-value">{total_app}</div></div>
        <div class="stat-card"><div class="stat-icon"><i class="fas fa-clock"></i></div><div class="stat-label">Pending</div><div class="stat-value">{pending}</div></div>
        <div class="stat-card"><div class="stat-icon"><i class="fas fa-envelope"></i></div><div class="stat-label">Emailed</div><div class="stat-value">{emailed}</div></div>
        <div class="stat-card" {'style="border-color: rgba(220,38,38,0.4); background: rgba(220,38,38,0.04);"' if email_failed > 0 else ''}><div class="stat-icon"><i class="fas fa-envelope-open" style="{'color:#dc2626;' if email_failed > 0 else ''}"></i></div><div class="stat-label" style="{'color:#dc2626;' if email_failed > 0 else ''}">Email Failed</div><div class="stat-value" style="{'color:#dc2626;' if email_failed > 0 else ''}">{email_failed}</div></div>
        <div class="stat-card"><div class="stat-icon"><i class="fas fa-check"></i></div><div class="stat-label">Codes Available</div><div class="stat-value">{codes_avail}</div></div>
        <div class="stat-card"><div class="stat-icon"><i class="fas fa-lock"></i></div><div class="stat-label">Codes Used</div><div class="stat-value">{codes_used}</div></div>
    </div>
    """

    clients_html = """<div class="card"><div class="card-title"><i class="fas fa-building"></i> Top Clients</div><table><thead><tr><th>Company</th><th>Account</th><th>Applicants</th></tr></thead><tbody>"""
    for c in clients:
        # FIX: Dashboard link was /clients/{id} (404) — corrected to query-param format /clients?client_id={id}
        clients_html += f'<tr><td><a href="/clients?client_id={c["id"]}" style="color: var(--primary); text-decoration: none;">{h(c["company_name"])}</a></td><td>{h(c["account_name"] or "-")}</td><td>{c["app_count"]}</td></tr>'
    clients_html += "</tbody></table></div>"

    activity_html = """<div class="card"><div class="card-title"><i class="fas fa-history"></i> Recent Activity</div><table><thead><tr><th>Type</th><th>Details</th><th>Time</th></tr></thead><tbody>"""
    for a in activity:
        if a["type"] == "new_applicant":
            activity_html += f'<tr><td><span class="status-badge status-pending">New</span></td><td>{h(a["col1"])} {h(a["col2"])}</td><td>{fmt_dt(a["ts"])}</td></tr>'
        elif a["type"] == "email_sent":
            activity_html += f'<tr><td><span class="status-badge status-emailed">Email</span></td><td>{h(a["col1"] or "-")} - {h(a["col2"] or "-")}</td><td>{fmt_dt(a["ts"])}</td></tr>'
        elif a["type"] == "email_failed":
            activity_html += f'<tr><td><span class="status-badge status-email_failed">Failed</span></td><td>{h(a["col1"] or "-")} &mdash; {h((a["col2"] or "Unknown error")[:80])}</td><td>{fmt_dt(a["ts"])}</td></tr>'
    activity_html += "</tbody></table></div>"

    return render_page("Dashboard", stats_html + clients_html + activity_html, active="dashboard", user=nav_user)


def page_applicants(db, params, nav_user=None):
    """List and manage applicants."""
    search = (params.get("search", [None])[0] or "").lower()
    rows = db.execute("SELECT * FROM applicants ORDER BY created_at DESC").fetchall()
    if search:
        rows = [r for r in rows if search in f"{r['first_name']} {r['last_name']} {r['accio_order_number'] or ''}".lower()]

    content = f"""
    <div style="margin-bottom: 1rem; display: flex; gap: 0.5rem;">
        <form method="GET" style="flex: 1; display: flex; gap: 0.5rem;">
            <input type="text" name="search" placeholder="Search by name or order #..." style="flex: 1;" value="{h(search)}">
            <button type="submit" class="btn btn-primary"><i class="fas fa-search"></i> Search</button>
        </form>
        <a href="/applicants/add" class="btn btn-primary"><i class="fas fa-plus"></i> Add Applicant</a>
        <form method="POST" action="/applicants/bulk-process" style="margin: 0;">
            <button type="submit" class="btn btn-success"><i class="fas fa-rocket"></i> Bulk Process Pending</button>
        </form>
    </div>
    <div class="card">
        <table>
            <thead><tr><th>Status</th><th>Order #</th><th>Name</th><th>DOB</th><th>Email</th><th>Code</th><th>Email Status</th><th>Actions</th></tr></thead>
            <tbody>
    """

    for r in rows:
        email_status = ""
        if not r["email_sent"]:
            email_status = '<span class="email-status email-status-unsent"></span> Not Sent'
        elif r["status"] == "email_failed":
            email_status = '<span class="email-status" style="background:#dc2626;"></span> Failed'
        else:
            sent_at = r.get("email_sent_at")
            sent_label = ""
            if sent_at:
                try:
                    dt = datetime.fromisoformat(str(sent_at)) if not isinstance(sent_at, datetime) else sent_at
                    sent_label = f' <span style="color:#999;font-size:0.75rem;">({dt.strftime("%m/%d %I:%M%p").lower()})</span>'
                except Exception:
                    pass
            email_status = f'<span class="email-status email-status-opened"></span> Sent{sent_label}'

        # Sanitize status value used in CSS class to prevent class injection
        safe_status = h(r['status']).replace(" ", "_")
        order_num = h(r['accio_order_number'] or '-')
        content += f"""
                <tr>
                    <td><span class="status-badge status-{safe_status}">{h(r['status'])}</span></td>
                    <td><code style="font-size:0.8rem;">{order_num}</code></td>
                    <td>{h(r['first_name'])} {h(r['last_name'])}</td>
                    <td style="font-size:0.85rem;">{h(r.get('date_of_birth') or '-')}</td>
                    <td>
                        <form method="POST" action="/applicants/{r['id']}/update-email" style="display:flex; gap:4px; align-items:center;">
                            <input type="email" name="email" value="{h(r['email'] or '')}" placeholder="Enter email" style="width:180px; padding:4px 8px; font-size:0.85rem; border:1px solid #ccc; border-radius:4px;">
                            <button type="submit" class="btn btn-small" style="padding:4px 8px; font-size:0.75rem; background:#6c757d; color:white; border:none; border-radius:4px;" title="Save email"><i class="fas fa-save"></i></button>
                        </form>
                    </td>
                    <td><code>{h(r['assigned_code'] or '-')}</code></td>
                    <td>{email_status}</td>
                    <td style="white-space: nowrap;">
                        <form method="POST" action="/applicants/{r['id']}/assign-and-send" style="display:inline;">
                            <button type="submit" class="btn btn-primary btn-small"><i class="fas fa-envelope"></i> Assign &amp; Send</button>
                        </form>
                        <form method="POST" action="/applicants/{r['id']}/resend" style="display:inline;">
                            <button type="submit" class="btn btn-small" style="background:#17a2b8; color:white;" title="Resend email"><i class="fas fa-redo"></i> Resend</button>
                        </form>
                        <form method="POST" action="/applicants/{r['id']}/delete" style="display:inline;" onsubmit="return confirm('Delete this applicant?')">
                            <button type="submit" class="btn btn-danger btn-small"><i class="fas fa-trash"></i></button>
                        </form>
                    </td>
                </tr>
        """

    content += "</tbody></table></div>"
    return render_page("Applicants", content, active="applicants", user=nav_user)


def page_add_applicant(nav_user=None):
    content = """
    <div class="card">
        <form method="POST" action="/applicants/add">
            <div class="grid-2">
                <div class="form-group"><label for="first_name">First Name</label><input type="text" id="first_name" name="first_name" required></div>
                <div class="form-group"><label for="last_name">Last Name</label><input type="text" id="last_name" name="last_name" required></div>
            </div>
            <div class="grid-2">
                <div class="form-group"><label for="email">Email</label><input type="email" id="email" name="email"></div>
                <div class="form-group"><label for="phone">Phone</label><input type="text" id="phone" name="phone"></div>
            </div>
            <div class="grid-2">
                <div class="form-group"><label for="date_of_birth">Date of Birth</label><input type="date" id="date_of_birth" name="date_of_birth"></div>
                <div class="form-group"><label for="last_four_ssn">Last 4 SSN</label><input type="text" id="last_four_ssn" name="last_four_ssn" maxlength="4" pattern="[0-9]{4}" placeholder="1234"></div>
            </div>
            <div class="form-group"><label for="accio_order_number">Accio Order Number</label><input type="text" id="accio_order_number" name="accio_order_number"></div>
            <div class="form-group"><label for="notes">Notes</label><textarea id="notes" name="notes" style="min-height: 100px;"></textarea></div>
            <button type="submit" class="btn btn-primary"><i class="fas fa-save"></i> Add Applicant</button>
            <a href="/applicants" class="btn" style="background: var(--gray-300); color: var(--gray-900);">Cancel</a>
        </form>
    </div>
    """
    return render_page("Add Applicant", content, active="applicants", user=nav_user)


def page_codes(db, params, nav_user=None):
    avail = db.execute("SELECT COUNT(*) as cnt FROM codes WHERE assigned_to IS NULL").fetchone()["cnt"]
    assigned = db.execute("SELECT COUNT(*) as cnt FROM codes WHERE assigned_to IS NOT NULL").fetchone()["cnt"]
    rows = db.execute("SELECT * FROM codes ORDER BY imported_at DESC LIMIT 100").fetchall()

    content = f"""
    <div style="margin-bottom: 1rem; display: flex; gap: 0.5rem;">
        <a href="/codes/import" class="btn btn-primary"><i class="fas fa-upload"></i> Import from File</a>
        <a href="/codes/manual" class="btn btn-primary"><i class="fas fa-plus"></i> Add Manually</a>
    </div>
    <div class="stats">
        <div class="stat-card"><div class="stat-icon"><i class="fas fa-check"></i></div><div class="stat-label">Available</div><div class="stat-value">{avail}</div></div>
        <div class="stat-card"><div class="stat-icon"><i class="fas fa-lock"></i></div><div class="stat-label">Assigned</div><div class="stat-value">{assigned}</div></div>
    </div>
    <div class="card">
        <table>
            <thead><tr><th>Code</th><th>Status</th><th>Batch</th><th>Assigned To</th><th>Date</th><th>Actions</th></tr></thead>
            <tbody>
    """

    # FIX: Was slicing rows[:50] from a LIMIT 100 query — now consistently shows all 100
    for r in rows:
        assigned_to = "-"
        if r["assigned_to"]:
            a = db.execute("SELECT first_name, last_name FROM applicants WHERE id = %s",
                           (r["assigned_to"],)).fetchone()
            if a:
                assigned_to = f"{h(a['first_name'])} {h(a['last_name'])}"

        delete_btn = (
            f'<form method="POST" action="/codes/{r["id"]}/delete" style="display:inline;" '
            f'onsubmit="return confirm(\'Delete this code?\')"><button type="submit" class="btn btn-small" '
            f'style="padding:4px 8px; font-size:0.75rem; background:#dc3545; color:white; border:none; '
            f'border-radius:4px;" title="Delete code"><i class="fas fa-trash"></i></button></form>'
            if not r["assigned_to"]
            else '<span style="color:#999; font-size:0.8rem;">In use</span>'
        )

        content += f"""
                <tr>
                    <td><code>{h(r['code'])}</code></td>
                    <td><span class="status-badge {'status-pending' if not r['assigned_to'] else 'status-code_assigned'}">{h(r['status'])}</span></td>
                    <td>{h(r['batch_name'] or '-')}</td>
                    <td>{assigned_to}</td>
                    <td>{fmt_dt(r['imported_at'])}</td>
                    <td>{delete_btn}</td>
                </tr>
        """

    content += "</tbody></table></div>"
    return render_page("Payment Codes", content, active="codes", user=nav_user)


def page_import_codes(nav_user=None):
    content = """
    <div class="card">
        <form method="POST" action="/codes/import" enctype="multipart/form-data">
            <div class="form-group"><label for="file">Excel or CSV File</label><input type="file" id="file" name="file" accept=".xlsx,.csv" required></div>
            <div class="form-group"><label for="column_index">Column Number (0-indexed)</label><input type="number" id="column_index" name="column_index" value="0" min="0"></div>
            <div class="form-group"><label><input type="checkbox" name="skip_header" checked> Skip first row (header)</label></div>
            <div class="form-group"><label for="batch_name">Batch Name</label><input type="text" id="batch_name" name="batch_name" placeholder="e.g., 'January 2024 Import'"></div>
            <button type="submit" class="btn btn-primary"><i class="fas fa-upload"></i> Import Codes</button>
            <a href="/codes" class="btn" style="background: var(--gray-300); color: var(--gray-900);">Cancel</a>
        </form>
    </div>
    """
    return render_page("Import Payment Codes", content, active="codes", user=nav_user)


def page_settings(db, nav_user=None):
    # FIX: Replaced positional .format() on a template with explicit named variables
    # to prevent KeyError crashes if setting values contain curly braces
    smtp_server_val = h(get_setting(db, "smtp_server"))
    smtp_port_val = h(get_setting(db, "smtp_port"))
    smtp_user_val = h(get_setting(db, "smtp_username"))
    smtp_pass_val = h(get_setting(db, "smtp_password"))
    tls_checked = 'checked' if get_setting(db, "smtp_use_tls") == "1" else ''
    sender_email_val = h(get_setting(db, "sender_email"))
    sender_name_val = h(get_setting(db, "sender_name"))
    email_subj_val = h(get_setting(db, "email_subject"))
    email_body_val = h(get_setting(db, "email_body"))
    accio_account_val = h(get_setting(db, "accio_account"))
    accio_username_val = h(get_setting(db, "accio_username"))
    accio_password_val = h(get_setting(db, "accio_password"))
    accio_researcher_url_val = h(get_setting(db, "accio_researcher_url"))
    # SMS / Twilio settings
    twilio_sid_val = h(get_setting(db, "twilio_account_sid"))
    twilio_token_val = h(get_setting(db, "twilio_auth_token"))
    twilio_from_val = h(get_setting(db, "twilio_from_number"))
    auto_sms_checked = 'checked' if get_setting(db, "auto_send_sms") == "1" else ''
    sms_body_val = h(get_setting(db, "sms_body"))
    twilio_installed = "Installed" if HAS_TWILIO else "NOT INSTALLED - run: pip install twilio"
    twilio_badge_color = "var(--success)" if HAS_TWILIO else "var(--danger, #e74c3c)"

    content = f"""
    <div class="card">
        <div class="card-title"><i class="fas fa-exchange-alt"></i> Accio Postback Configuration</div>
        <form method="POST" action="/settings">
            <p style="color: var(--gray-500); font-size: 0.875rem; margin-bottom: 1rem;">
                When an FP Release email is sent, the app will automatically post a status note
                back to Accio (Chapter 9 &amp; 11 of the XML Integration Manual) so the screening
                firm can see the email was sent and the assigned code.
                Leave <strong>Researcher URL</strong> blank to disable postback.
            </p>
            <div class="grid-2">
                <div class="form-group"><label for="accio_account">Accio Account</label><input type="text" id="accio_account" name="accio_account" value="{accio_account_val}"></div>
                <div class="form-group"><label for="accio_username">Accio Username</label><input type="text" id="accio_username" name="accio_username" value="{accio_username_val}"></div>
            </div>
            <div class="grid-2">
                <div class="form-group"><label for="accio_password">Accio Password</label><input type="password" id="accio_password" name="accio_password" value="{accio_password_val}"></div>
                <div class="form-group"><label for="accio_researcher_url">Researcher XML URL</label><input type="text" id="accio_researcher_url" name="accio_researcher_url" value="{accio_researcher_url_val}" placeholder="https://yourcompany.acciodata.com/c/p/researcherxml"></div>
            </div>
            <button type="submit" class="btn btn-primary"><i class="fas fa-save"></i> Save Accio Settings</button>
        </form>
    </div>
    <div class="card">
        <div class="card-title"><i class="fas fa-cog"></i> SMTP Configuration</div>
        <form method="POST" action="/settings">
            <div class="grid-2">
                <div class="form-group"><label for="smtp_server">SMTP Server</label><input type="text" id="smtp_server" name="smtp_server" value="{smtp_server_val}" required></div>
                <div class="form-group"><label for="smtp_port">SMTP Port</label><input type="number" id="smtp_port" name="smtp_port" value="{smtp_port_val}" required></div>
            </div>
            <div class="grid-2">
                <div class="form-group"><label for="smtp_username">SMTP Username</label><input type="text" id="smtp_username" name="smtp_username" value="{smtp_user_val}"></div>
                <div class="form-group"><label for="smtp_password">SMTP Password</label><input type="password" id="smtp_password" name="smtp_password" value="{smtp_pass_val}"></div>
            </div>
            <div class="form-group">
                <label>
                    <input type="hidden" name="smtp_use_tls" value="0">
                    <input type="checkbox" name="smtp_use_tls" value="1" {tls_checked}>
                    Use TLS (Recommended)
                </label>
            </div>
            <div class="grid-2">
                <div class="form-group"><label for="sender_email">Sender Email</label><input type="email" id="sender_email" name="sender_email" value="{sender_email_val}" required></div>
                <div class="form-group"><label for="sender_name">Sender Name</label><input type="text" id="sender_name" name="sender_name" value="{sender_name_val}"></div>
            </div>
            <div class="form-group"><label for="email_subject">Email Subject Template</label><input type="text" id="email_subject" name="email_subject" value="{email_subj_val}"></div>
            <div class="form-group">
                <label for="email_body">Email Body Template</label>
                <textarea id="email_body" name="email_body" style="min-height: 300px;">{email_body_val}</textarea>
                <p style="margin-top: 0.5rem; color: var(--gray-500); font-size: 0.875rem;">
                    Available placeholders: {{first_name}}, {{last_name}}, {{code}}, {{company_name}}, {{ori_number}}
                </p>
            </div>
            <button type="submit" class="btn btn-primary"><i class="fas fa-save"></i> Save Settings</button>
        </form>
    </div>
    <div class="card">
        <div class="card-title"><i class="fas fa-sms"></i> SMS Configuration (Twilio)</div>
        <p style="color: var(--gray-500); font-size: 0.875rem; margin-bottom: 0.5rem;">
            Send text messages to applicants alongside emails. SMS includes their IdentoGO code and scheduling instructions.
        </p>
        <p style="font-size: 0.8rem; margin-bottom: 1rem;">
            Twilio SDK: <span style="color: {twilio_badge_color}; font-weight: bold;">{twilio_installed}</span>
        </p>
        <form method="POST" action="/settings">
            <div class="grid-2">
                <div class="form-group"><label for="twilio_account_sid">Twilio Account SID</label><input type="text" id="twilio_account_sid" name="twilio_account_sid" value="{twilio_sid_val}" placeholder="ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"></div>
                <div class="form-group"><label for="twilio_auth_token">Twilio Auth Token</label><input type="password" id="twilio_auth_token" name="twilio_auth_token" value="{twilio_token_val}"></div>
            </div>
            <div class="grid-2">
                <div class="form-group"><label for="twilio_from_number">Twilio Phone Number (From)</label><input type="text" id="twilio_from_number" name="twilio_from_number" value="{twilio_from_val}" placeholder="+12255551234"></div>
                <div class="form-group">
                    <label>
                        <input type="hidden" name="auto_send_sms" value="0">
                        <input type="checkbox" name="auto_send_sms" value="1" {auto_sms_checked}>
                        Auto-send SMS when code is assigned
                    </label>
                </div>
            </div>
            <div class="form-group">
                <label for="sms_body">SMS Body Template</label>
                <textarea id="sms_body" name="sms_body" style="min-height: 200px;">{sms_body_val}</textarea>
                <p style="margin-top: 0.5rem; color: var(--gray-500); font-size: 0.875rem;">
                    Available placeholders: {{first_name}}, {{last_name}}, {{code}}, {{company_name}}, {{ori_number}}<br>
                    <strong>Tip:</strong> Keep SMS under 320 characters for best delivery (2 SMS segments max).
                </p>
            </div>
            <button type="submit" class="btn btn-primary"><i class="fas fa-save"></i> Save SMS Settings</button>
        </form>
        <hr style="margin: 1rem 0;">
        <form method="POST" action="/settings/test-sms" style="display: flex; gap: 0.5rem; align-items: end;">
            <div class="form-group" style="flex: 1; margin-bottom: 0;">
                <label for="test_phone">Test SMS</label>
                <input type="text" id="test_phone" name="test_phone" placeholder="+12255551234 or (225) 555-1234">
            </div>
            <button type="submit" class="btn" style="background: #25D366; color: white; white-space: nowrap;"><i class="fas fa-paper-plane"></i> Send Test SMS</button>
        </form>
    </div>
    """
    return render_page("Settings", content, active="settings", user=nav_user)


def page_logs(db, nav_user=None):
    xml_rows = db.execute("SELECT * FROM xml_log ORDER BY id DESC LIMIT 50").fetchall()
    email_rows = db.execute("SELECT * FROM email_log ORDER BY id DESC LIMIT 50").fetchall()
    sms_rows = db.execute("SELECT * FROM sms_log ORDER BY id DESC LIMIT 50").fetchall()

    xml_html = """<div class="card"><div class="card-title"><i class="fas fa-code"></i> XML Logs (Accio Push)</div>
    <table><thead><tr><th>ID</th><th>Direction</th><th>Status</th><th>Error</th><th>Received</th></tr></thead><tbody>"""
    for r in xml_rows:
        xml_html += f"<tr><td>{r['id']}</td><td>{h(r['direction'] or '-')}</td><td>{h(r['parsed_status'] or '-')}</td><td>{h((r['error_message'] or '')[:60])}</td><td>{fmt_dt(r['received_at'])}</td></tr>"
    xml_html += "</tbody></table></div>"

    email_html = """<div class="card"><div class="card-title"><i class="fas fa-envelope"></i> Email Logs</div>
    <table><thead><tr><th>ID</th><th>Recipient</th><th>Subject</th><th>Status</th><th>Error</th><th>Sent</th></tr></thead><tbody>"""
    for r in email_rows:
        email_html += f"<tr><td>{r['id']}</td><td>{h(r['recipient_email'] or '-')}</td><td>{h((r['subject'] or '')[:40])}</td><td>{h(r['status'] or '-')}</td><td>{h((r['error_message'] or '')[:40])}</td><td>{fmt_dt(r['sent_at'])}</td></tr>"
    email_html += "</tbody></table></div>"

    sms_html = """<div class="card"><div class="card-title"><i class="fas fa-sms"></i> SMS Logs</div>
    <table><thead><tr><th>ID</th><th>Recipient</th><th>Twilio SID</th><th>Status</th><th>Error</th><th>Sent</th></tr></thead><tbody>"""
    for r in sms_rows:
        sms_html += f"<tr><td>{r['id']}</td><td>{h(r['recipient_phone'] or '-')}</td><td><code style='font-size:0.75rem;'>{h((r['twilio_sid'] or '-')[:20])}</code></td><td>{h(r['status'] or '-')}</td><td>{h((r['error_message'] or '')[:40])}</td><td>{fmt_dt(r['sent_at'])}</td></tr>"
    sms_html += "</tbody></table></div>"

    return render_page("Logs", xml_html + email_html + sms_html, active="logs", user=nav_user)


def page_clients(db, params, nav_user=None):
    # FIX: Wrapped int(client_id) in try/except to prevent ValueError 500 crash
    client_id_raw = params.get("client_id", [None])[0]
    client_id = None
    if client_id_raw:
        try:
            client_id = int(client_id_raw)
        except (ValueError, TypeError):
            return render_page("Invalid Request",
                               '<div class="es"><i class="fas fa-exclamation-triangle"></i><h3>Invalid client ID</h3></div>',
                               active="clients", user=nav_user)

    if client_id:
        client = db.execute("SELECT * FROM clients WHERE id = %s", (client_id,)).fetchone()
        if not client:
            return render_page("Not Found",
                               '<div class="es"><i class="fas fa-exclamation-triangle"></i><h3>Client not found</h3></div>',
                               active="clients", user=nav_user)

        applicants = db.execute(
            "SELECT * FROM applicants WHERE client_id = %s ORDER BY created_at DESC",
            (client_id,)
        ).fetchall()

        content = f"""
        <a href="/clients" class="btn" style="background: var(--gray-300); color: var(--gray-900); margin-bottom: 1rem;"><i class="fas fa-arrow-left"></i> Back</a>
        <div class="card">
            <div class="card-title">{h(client['company_name'])}</div>
            <p><strong>Account:</strong> {h(client['account_name'] or '-')}</p>
            <p><strong>Email:</strong> {h(client['contact_email'] or '-')}</p>
            <p><strong>Phone:</strong> {h(client['contact_phone'] or '-')}</p>
            <p><strong>Total Applicants:</strong> {len(applicants)}</p>
        </div>
        <div class="card"><div class="card-title">Applicants</div>
            <table><thead><tr><th>Order #</th><th>Name</th><th>Email</th><th>Status</th><th>Code</th></tr></thead><tbody>
        """
        for a in applicants:
            safe_st = h(a['status']).replace(" ", "_")
            content += f"<tr><td><code style='font-size:0.8rem;'>{h(a['accio_order_number'] or '-')}</code></td><td>{h(a['first_name'])} {h(a['last_name'])}</td><td>{h(a['email'] or '-')}</td><td><span class=\"status-badge status-{safe_st}\">{h(a['status'])}</span></td><td><code>{h(a['assigned_code'] or '-')}</code></td></tr>"
        content += "</tbody></table></div>"
    else:
        clients = db.execute("""
            SELECT c.*, COUNT(a.id) as app_count, MAX(a.created_at) as last_order
            FROM clients c
            LEFT JOIN applicants a ON a.client_id = c.id
            GROUP BY c.id ORDER BY app_count DESC
        """).fetchall()

        content = """<div class="card"><table><thead><tr><th>Company</th><th>Account</th><th>Contact Email</th><th>Total Applicants</th><th>Last Order</th><th>Actions</th></tr></thead><tbody>"""
        for c in clients:
            content += f'<tr><td>{h(c["company_name"])}</td><td>{h(c["account_name"] or "-")}</td><td>{h(c["contact_email"] or "-")}</td><td>{c["app_count"]}</td><td>{fmt_dt(c["last_order"])}</td><td><a href="/clients?client_id={c["id"]}" class="btn btn-primary btn-small">View</a></td></tr>'
        content += "</tbody></table></div>"

    return render_page("Clients", content, active="clients", user=nav_user)


# ---------------------------------------------------------------------------
# Password Reset
# ---------------------------------------------------------------------------
def send_password_reset_email(db, username_or_email):
    """Look up user by username or recovery_email, send a reset link."""
    user = db.execute(
        "SELECT * FROM users WHERE (username=%s OR recovery_email=%s) AND is_active=TRUE LIMIT 1",
        (username_or_email, username_or_email)
    ).fetchone()
    if not user:
        # Do not reveal whether the account exists
        return True, "If a matching account is found, a reset email will be sent."

    recovery_addr = (user.get("recovery_email") or "").strip()
    if not recovery_addr:
        return False, "No recovery email is configured for this account. Contact an administrator."

    token = str(uuid.uuid4())
    expires_at = (datetime.now() + timedelta(hours=2)).isoformat()
    db.execute(
        "INSERT INTO password_reset_tokens (user_id, token, expires_at) VALUES (%s, %s, %s)",
        (user["id"], token, expires_at)
    )

    smtp_server = get_setting(db, "smtp_server")
    smtp_port = int(get_setting(db, "smtp_port") or 587)
    smtp_user = get_setting(db, "smtp_username")
    smtp_pass = get_setting(db, "smtp_password")
    use_tls = get_setting(db, "smtp_use_tls") == "1"
    sender_email = get_setting(db, "sender_email")
    sender_name = get_setting(db, "sender_name") or "Fingerprint Release Manager"

    base_url = (get_setting(db, "accio_post_url") or "").rstrip("/").rsplit("/api/", 1)[0] or "http://localhost:5000"
    reset_link = f"{base_url}/reset-password?token={token}"

    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = f"{sender_name} <{sender_email}>"
        msg["To"] = recovery_addr
        msg["Subject"] = "Password Reset – Fingerprint Release Manager"
        plain = (f"Hi {user['display_name'] or user['username']},\n\n"
                 f"A password reset was requested for your account.\n\n"
                 f"Click the link below to reset your password (valid for 2 hours):\n{reset_link}\n\n"
                 f"If you did not request this, you can safely ignore this email.\n\n"
                 f"— Fingerprint Release Manager")
        html_body = f"""<!DOCTYPE html><html><head><meta charset="utf-8"></head>
<body style="font-family:Arial,sans-serif;color:#333;max-width:500px;margin:0 auto;">
<h2 style="color:#2563eb;">Password Reset</h2>
<p>Hi <strong>{h(user['display_name'] or user['username'])}</strong>,</p>
<p>A password reset was requested for your account. Click the button below to set a new password.
This link is valid for <strong>2 hours</strong>.</p>
<p style="text-align:center;margin:2rem 0;">
  <a href="{h(reset_link)}" style="background:#2563eb;color:white;padding:0.75rem 2rem;
     border-radius:0.375rem;text-decoration:none;font-weight:600;">Reset Password</a>
</p>
<p style="color:#666;font-size:0.875rem;">Or copy this link into your browser:<br>
<code style="word-break:break-all;">{h(reset_link)}</code></p>
<p style="color:#666;font-size:0.875rem;">If you did not request this, you can safely ignore this email.</p>
</body></html>"""
        msg.attach(MIMEText(plain, "plain"))
        msg.attach(MIMEText(html_body, "html"))
        srv = smtplib.SMTP(smtp_server, smtp_port, timeout=15)
        if use_tls:
            srv.starttls()
        if smtp_user and smtp_pass:
            srv.login(smtp_user, smtp_pass)
        srv.send_message(msg)
        srv.quit()
        logger.info(f"Password reset email sent to {recovery_addr} for user {user['username']}")
        return True, "Reset email sent. Check your inbox."
    except Exception as e:
        logger.error(f"Password reset email failed: {e}")
        return False, f"Could not send email: {e}"


def page_forgot_password(error="", success=""):
    error_html = f'<div class="alert alert-error">{h(error)}</div>' if error else ""
    success_html = f'<div class="alert alert-success">{h(success)}</div>' if success else ""
    return f"""<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Forgot Password – Fingerprint Release Manager</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
<style>
@keyframes glow-pulse{{0%,100%{{filter:drop-shadow(0 0 8px rgba(0,212,255,0.6));}}50%{{filter:drop-shadow(0 0 20px rgba(0,212,255,0.9));}}}}
@keyframes float-in{{from{{opacity:0;transform:translateY(20px);}}to{{opacity:1;transform:translateY(0);}}}}
:root{{--primary:#00d4ff;--accent-purple:#a855f7;--surface-0:#0a0e1a;--surface-1:rgba(15,23,42,0.85);--text-primary:#e2e8f0;--text-muted:#64748b;}}
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:var(--surface-0);background-image:radial-gradient(ellipse at 20% 50%,rgba(0,212,255,0.08) 0%,transparent 50%),radial-gradient(ellipse at 80% 50%,rgba(168,85,247,0.06) 0%,transparent 50%);min-height:100vh;display:flex;align-items:center;justify-content:center;}}
.card{{background:var(--surface-1);backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px);border:1px solid rgba(0,212,255,0.15);border-radius:1rem;box-shadow:0 10px 40px rgba(0,0,0,0.5),0 0 60px rgba(0,212,255,0.05);padding:3rem;width:100%;max-width:420px;animation:float-in 0.6s ease-out;}}
.brand{{text-align:center;margin-bottom:2rem;}}
.brand i{{font-size:3rem;background:linear-gradient(135deg,var(--primary),var(--accent-purple));-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:.5rem;animation:glow-pulse 3s ease-in-out infinite;}}
.brand h1{{font-size:1.5rem;color:var(--text-primary);margin:0;font-weight:600;}}
.brand p{{color:var(--text-muted);margin:.5rem 0 0;font-size:.875rem;letter-spacing:0.05em;}}
.fg{{margin-bottom:1.5rem;}}
label{{display:block;margin-bottom:.5rem;font-weight:500;color:var(--text-primary);font-size:.875rem;}}
input{{width:100%;padding:.75rem 1rem;border:1px solid rgba(0,212,255,0.2);border-radius:.5rem;font-size:.875rem;font-family:inherit;background:rgba(0,0,0,0.3);color:var(--text-primary);transition:all 0.3s ease;}}
input:focus{{outline:none;border-color:var(--primary);box-shadow:0 0 0 3px rgba(0,212,255,0.15),0 0 20px rgba(0,212,255,0.1);}}
input::placeholder{{color:var(--text-muted);}}
.btn{{width:100%;padding:.75rem;background:linear-gradient(135deg,var(--primary),var(--accent-purple));color:white;border:none;border-radius:.5rem;font-size:.875rem;font-weight:600;cursor:pointer;transition:all 0.3s ease;letter-spacing:0.03em;}}
.btn:hover{{box-shadow:0 0 25px rgba(0,212,255,0.3),0 0 50px rgba(168,85,247,0.15);transform:translateY(-1px);}}
.back{{display:block;text-align:center;margin-top:1rem;color:var(--primary);font-size:.875rem;text-decoration:none;transition:color 0.2s;}}
.back:hover{{color:#a855f7;}}
.alert{{padding:.75rem;border-radius:.5rem;margin-bottom:1.5rem;font-size:.875rem;}}
.alert-error{{background:rgba(239,68,68,0.1);border:1px solid rgba(239,68,68,0.4);color:#f87171;}}
.alert-success{{background:rgba(16,185,129,0.1);border:1px solid rgba(16,185,129,0.4);color:#34d399;}}
</style></head><body>
<div class="card">
  <div class="brand"><i class="fas fa-fingerprint"></i><h1>Forgot Password</h1><p>Enter your username or recovery email</p></div>
  {error_html}{success_html}
  {'<form method="POST" action="/forgot-password"><div class="fg"><label for="identifier">Username or Recovery Email</label><input type="text" id="identifier" name="identifier" required autofocus></div><button type="submit" class="btn"><i class="fas fa-paper-plane"></i> Send Reset Link</button></form>' if not success else ''}
  <a href="/login" class="back"><i class="fas fa-arrow-left"></i> Back to Login</a>
</div></body></html>"""


def page_reset_password(token, error=""):
    error_html = f'<div class="alert" style="background:rgba(239,68,68,0.1);border:1px solid rgba(239,68,68,0.4);color:#f87171;padding:.75rem;border-radius:.5rem;margin-bottom:1.5rem;font-size:.875rem;">{h(error)}</div>' if error else ""
    return f"""<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Reset Password – Fingerprint Release Manager</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
<style>
@keyframes glow-pulse{{0%,100%{{filter:drop-shadow(0 0 8px rgba(0,212,255,0.6));}}50%{{filter:drop-shadow(0 0 20px rgba(0,212,255,0.9));}}}}
@keyframes float-in{{from{{opacity:0;transform:translateY(20px);}}to{{opacity:1;transform:translateY(0);}}}}
:root{{--primary:#00d4ff;--accent-purple:#a855f7;--surface-0:#0a0e1a;--surface-1:rgba(15,23,42,0.85);--text-primary:#e2e8f0;--text-muted:#64748b;}}
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:var(--surface-0);background-image:radial-gradient(ellipse at 20% 50%,rgba(0,212,255,0.08) 0%,transparent 50%),radial-gradient(ellipse at 80% 50%,rgba(168,85,247,0.06) 0%,transparent 50%);min-height:100vh;display:flex;align-items:center;justify-content:center;}}
.card{{background:var(--surface-1);backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px);border:1px solid rgba(0,212,255,0.15);border-radius:1rem;box-shadow:0 10px 40px rgba(0,0,0,0.5),0 0 60px rgba(0,212,255,0.05);padding:3rem;width:100%;max-width:420px;animation:float-in 0.6s ease-out;}}
.brand{{text-align:center;margin-bottom:2rem;}}
.brand i{{font-size:3rem;background:linear-gradient(135deg,var(--primary),var(--accent-purple));-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:.5rem;animation:glow-pulse 3s ease-in-out infinite;}}
.brand h1{{font-size:1.5rem;color:var(--text-primary);margin:0;font-weight:600;}}
.fg{{margin-bottom:1.5rem;}}
label{{display:block;margin-bottom:.5rem;font-weight:500;color:var(--text-primary);font-size:.875rem;}}
input{{width:100%;padding:.75rem 1rem;border:1px solid rgba(0,212,255,0.2);border-radius:.5rem;font-size:.875rem;font-family:inherit;background:rgba(0,0,0,0.3);color:var(--text-primary);transition:all 0.3s ease;}}
input:focus{{outline:none;border-color:var(--primary);box-shadow:0 0 0 3px rgba(0,212,255,0.15),0 0 20px rgba(0,212,255,0.1);}}
input::placeholder{{color:var(--text-muted);}}
.btn{{width:100%;padding:.75rem;background:linear-gradient(135deg,var(--primary),var(--accent-purple));color:white;border:none;border-radius:.5rem;font-size:.875rem;font-weight:600;cursor:pointer;transition:all 0.3s ease;letter-spacing:0.03em;}}
.btn:hover{{box-shadow:0 0 25px rgba(0,212,255,0.3),0 0 50px rgba(168,85,247,0.15);transform:translateY(-1px);}}
.back{{display:block;text-align:center;margin-top:1rem;color:var(--primary);font-size:.875rem;text-decoration:none;transition:color 0.2s;}}
.back:hover{{color:#a855f7;}}
.hint{{color:var(--text-muted);font-size:.8rem;margin-top:.25rem;}}
</style></head><body>
<div class="card">
  <div class="brand"><i class="fas fa-key"></i><h1>Set New Password</h1></div>
  {error_html}
  <form method="POST" action="/reset-password">
    <input type="hidden" name="token" value="{h(token)}">
    <div class="fg">
      <label for="password">New Password</label>
      <input type="password" id="password" name="password" required minlength="8" autofocus>
      <p class="hint">Minimum 8 characters</p>
    </div>
    <div class="fg">
      <label for="confirm">Confirm Password</label>
      <input type="password" id="confirm" name="confirm" required minlength="8">
    </div>
    <button type="submit" class="btn"><i class="fas fa-lock"></i> Reset Password</button>
  </form>
  <a href="/login" class="back"><i class="fas fa-arrow-left"></i> Back to Login</a>
</div></body></html>"""


# ---------------------------------------------------------------------------
# User Management Pages (admin only)
# ---------------------------------------------------------------------------
def page_users(db, user):
    rows = db.execute(
        "SELECT id, username, display_name, role, is_active, recovery_email, created_at, last_login FROM users ORDER BY id"
    ).fetchall()
    content = """
    <div style="margin-bottom:1rem;display:flex;gap:.5rem;">
      <a href="/users/add" class="btn btn-primary"><i class="fas fa-user-plus"></i> Create User</a>
    </div>
    <div class="card">
      <table>
        <thead><tr><th>Username</th><th>Display Name</th><th>Role</th><th>Recovery Email</th><th>Status</th><th>Last Login</th><th>Actions</th></tr></thead>
        <tbody>
    """
    for r in rows:
        active_badge = ('<span class="status-badge status-emailed">Active</span>' if r["is_active"]
                        else '<span class="status-badge status-pending">Disabled</span>')
        toggle_label = "Disable" if r["is_active"] else "Enable"
        toggle_class = "btn-danger" if r["is_active"] else "btn-success"
        can_delete = r["id"] != user["id"] and r["role"] != "admin"
        _uname_escaped = h(r["username"])
        delete_btn = (
            f'<form method="POST" action="/users/{r["id"]}/delete" style="display:inline;" '
            f'onsubmit="return confirm(\'Delete user {_uname_escaped}? This cannot be undone.\');">'
            f'<button type="submit" class="btn btn-danger btn-small"><i class="fas fa-trash"></i></button></form>'
            if can_delete else ''
        )
        self_tag = ' <em style="color:#9ca3af;font-size:.75rem;">(you)</em>' if r["id"] == user["id"] else ""
        content += f"""
        <tr>
          <td><strong>{h(r["username"])}</strong>{self_tag}</td>
          <td>{h(r["display_name"] or "-")}</td>
          <td><span class="status-badge {'status-emailed' if r['role']=='admin' else 'status-code_assigned'}">{h(r["role"])}</span></td>
          <td>{h(r["recovery_email"] or "-")}</td>
          <td>{active_badge}</td>
          <td>{fmt_dt(r["last_login"])}</td>
          <td style="white-space:nowrap;">
            <form method="POST" action="/users/{r['id']}/toggle" style="display:inline;">
              <button type="submit" class="btn btn-small {toggle_class}">{toggle_label}</button>
            </form>
            <a href="/users/{r['id']}/reset" class="btn btn-small" style="background:#6c757d;color:white;">Reset PW</a>
            {delete_btn}
          </td>
        </tr>"""
    content += "</tbody></table></div>"
    return render_page("User Management", content, active="users", user=user)


def page_add_user(nav_user=None, error=""):
    error_html = f'<div class="alert alert-error"><i class="fas fa-exclamation-circle"></i> {h(error)}</div>' if error else ""
    content = f"""{error_html}
    <div class="card">
      <form method="POST" action="/users/add">
        <div class="grid-2">
          <div class="form-group"><label for="username">Username *</label><input type="text" id="username" name="username" required autocomplete="off"></div>
          <div class="form-group"><label for="display_name">Display Name</label><input type="text" id="display_name" name="display_name" autocomplete="off"></div>
        </div>
        <div class="grid-2">
          <div class="form-group"><label for="password">Password *</label><input type="password" id="password" name="password" required minlength="8" autocomplete="new-password"></div>
          <div class="form-group"><label for="role">Role</label>
            <select id="role" name="role">
              <option value="user">User</option>
              <option value="admin">Admin</option>
            </select>
          </div>
        </div>
        <div class="form-group"><label for="recovery_email">Recovery Email</label><input type="email" id="recovery_email" name="recovery_email" placeholder="user@example.com" autocomplete="off"></div>
        <button type="submit" class="btn btn-primary"><i class="fas fa-user-plus"></i> Create User</button>
        <a href="/users" class="btn" style="background:var(--gray-300);color:var(--gray-900);">Cancel</a>
      </form>
    </div>"""
    return render_page("Create User", content, active="users", user=nav_user)


def page_reset_user_password(user_id, username, nav_user=None, error=""):
    error_html = f'<div class="alert alert-error"><i class="fas fa-exclamation-circle"></i> {h(error)}</div>' if error else ""
    content = f"""{error_html}
    <div class="card">
      <p style="margin-bottom:1rem;">Set a new password for <strong>{h(username)}</strong>.</p>
      <form method="POST" action="/users/{user_id}/reset">
        <div class="form-group"><label for="new_password">New Password</label><input type="password" id="new_password" name="new_password" required minlength="8" autocomplete="new-password"></div>
        <div class="form-group"><label for="confirm_password">Confirm Password</label><input type="password" id="confirm_password" name="confirm_password" required minlength="8" autocomplete="new-password"></div>
        <button type="submit" class="btn btn-primary"><i class="fas fa-key"></i> Set Password</button>
        <a href="/users" class="btn" style="background:var(--gray-300);color:var(--gray-900);">Cancel</a>
      </form>
    </div>"""
    return render_page(f"Reset Password: {username}", content, active="users", user=nav_user)


# ---------------------------------------------------------------------------
# Profile / My Account Page (per-user recovery email)
# ---------------------------------------------------------------------------
def page_profile(db, user, error="", success=""):
    u = db.execute("SELECT * FROM users WHERE id=%s", (user["id"],)).fetchone()
    recovery_val = h(u.get("recovery_email") or "")
    display_val = h(u.get("display_name") or "")
    error_html = f'<div class="alert alert-error"><i class="fas fa-exclamation-circle"></i> {h(error)}</div>' if error else ""
    success_html = f'<div class="alert alert-success"><i class="fas fa-check-circle"></i> {h(success)}</div>' if success else ""
    content = f"""{error_html}{success_html}
    <div class="card">
      <div class="card-title"><i class="fas fa-user-cog"></i> My Account</div>
      <form method="POST" action="/profile">
        <div class="grid-2">
          <div class="form-group"><label>Username</label><input type="text" value="{h(u['username'])}" disabled style="background:#f3f4f6;"></div>
          <div class="form-group"><label for="display_name">Display Name</label><input type="text" id="display_name" name="display_name" value="{display_val}"></div>
        </div>
        <div class="form-group">
          <label for="recovery_email">Recovery Email</label>
          <input type="email" id="recovery_email" name="recovery_email" value="{recovery_val}" placeholder="your@email.com">
          <p style="margin-top:.5rem;color:var(--gray-500);font-size:.875rem;">Used to receive password reset links. Keep this updated.</p>
        </div>
        <button type="submit" class="btn btn-primary"><i class="fas fa-save"></i> Save Profile</button>
      </form>
    </div>
    <div class="card" style="margin-top:1.5rem;">
      <div class="card-title"><i class="fas fa-key"></i> Change Password</div>
      <form method="POST" action="/profile/change-password">
        <div class="form-group"><label for="current_password">Current Password</label><input type="password" id="current_password" name="current_password" required autocomplete="current-password"></div>
        <div class="grid-2">
          <div class="form-group"><label for="new_password">New Password</label><input type="password" id="new_password" name="new_password" required minlength="8" autocomplete="new-password"></div>
          <div class="form-group"><label for="confirm_password">Confirm New Password</label><input type="password" id="confirm_password" name="confirm_password" required minlength="8" autocomplete="new-password"></div>
        </div>
        <button type="submit" class="btn btn-primary"><i class="fas fa-lock"></i> Change Password</button>
      </form>
    </div>"""
    return render_page("My Account", content, active="profile", user=user)


# ---------------------------------------------------------------------------
# HTTP Handler
# ---------------------------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        logger.info(f"{self.client_address[0]} - {fmt % args}")

    def _send(self, code, body, ct="text/html; charset=utf-8"):
        # FIX: Added charset=utf-8 to Content-Type to correctly declare encoding
        encoded = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _redirect(self, url, set_cookie=None, clear_cookie=False):
        self.send_response(303)
        self.send_header("Location", url)
        if set_cookie:
            # FIX: Added SameSite=Lax to prevent CSRF attacks via cross-site form submissions
            self.send_header("Set-Cookie",
                             f"session_token={set_cookie}; Path=/; HttpOnly; SameSite=Lax; Max-Age=86400")
        if clear_cookie:
            # FIX: Properly expire the session cookie on logout
            self.send_header("Set-Cookie",
                             "session_token=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0; Expires=Thu, 01 Jan 1970 00:00:00 GMT")
        self.end_headers()

    def _parse_form(self):
        ct = self.headers.get("Content-Type", "")
        length = int(self.headers.get("Content-Length", 0))
        if "multipart/form-data" in ct:
            env = {"REQUEST_METHOD": "POST", "CONTENT_TYPE": ct, "CONTENT_LENGTH": str(length)}
            fs = cgi.FieldStorage(fp=self.rfile, headers=self.headers, environ=env)
            return fs
        else:
            body = self.rfile.read(length).decode("utf-8", errors="replace")
            return urllib.parse.parse_qs(body)

    def _check_auth(self):
        """Check if user is authenticated; return user dict or None."""
        cookie = self.headers.get("Cookie", "")
        token = get_session_from_cookie(cookie)
        db = get_db()
        user = verify_session(db, token) if token else None
        db.close()
        return user

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        params = urllib.parse.parse_qs(parsed.query)

        # Auth check for all non-public routes
        user = None
        if path != "/login" and not path.startswith("/api/track/"):
            user = self._check_auth()
            if not user:
                self._redirect("/login")
                return

        db = get_db()
        try:
            if path == "/login":
                self._send(200, page_login())

            elif path == "/forgot-password":
                self._send(200, page_forgot_password())

            elif path == "/reset-password":
                token = params.get("token", [None])[0] or ""
                if not token:
                    self._redirect("/forgot-password")
                    return
                # Validate token exists and is not expired/used
                row = db.execute(
                    "SELECT * FROM password_reset_tokens WHERE token=%s AND used=FALSE AND expires_at > NOW()",
                    (token,)
                ).fetchone()
                if not row:
                    self._send(200, page_forgot_password(error="This reset link is invalid or has expired. Please request a new one."))
                else:
                    self._send(200, page_reset_password(token))

            elif path == "/logout":
                # FIX: Logout now (1) deletes session from DB, (2) expires cookie,
                # (3) only sends ONE response (redirect only, not body+redirect)
                cookie = self.headers.get("Cookie", "")
                token = get_session_from_cookie(cookie)
                if token:
                    delete_session(db, token)
                self._redirect("/login", clear_cookie=True)

            elif path == "/":
                self._send(200, page_dashboard(db, nav_user=user))

            elif path == "/applicants":
                self._send(200, page_applicants(db, params, nav_user=user))

            elif path == "/applicants/add":
                self._send(200, page_add_applicant(nav_user=user))

            elif path == "/clients":
                self._send(200, page_clients(db, params, nav_user=user))

            elif path == "/codes":
                self._send(200, page_codes(db, params, nav_user=user))

            elif path == "/codes/import":
                self._send(200, page_import_codes(nav_user=user))

            elif path == "/codes/manual":
                content = """
                <div class="card">
                    <h2 style="margin-bottom:1rem;">Add Codes Manually</h2>
                    <form method="POST" action="/codes/add-manual">
                        <div style="margin-bottom: 1rem;">
                            <label><strong>Batch Name</strong></label>
                            <input type="text" name="batch_name" value="Manual" placeholder="Batch name" style="width:100%; padding:8px; border:1px solid #ccc; border-radius:4px; margin-top:4px;">
                        </div>
                        <div style="margin-bottom: 1rem;">
                            <label><strong>Codes</strong> (one per line)</label>
                            <textarea name="codes" rows="10" placeholder="Enter one code per line" style="width:100%; padding:8px; border:1px solid #ccc; border-radius:4px; margin-top:4px; font-family:monospace;"></textarea>
                        </div>
                        <button type="submit" class="btn btn-primary"><i class="fas fa-plus"></i> Add Codes</button>
                        <a href="/codes" class="btn" style="background: var(--gray-300); color: var(--gray-900); margin-left:8px;">Cancel</a>
                    </form>
                </div>
                """
                self._send(200, render_page("Add Codes Manually", content, active="codes", user=user))

            elif path == "/settings":
                self._send(200, page_settings(db, nav_user=user))

            elif path == "/profile":
                self._send(200, page_profile(db, user))

            elif path == "/users":
                if user.get("role") != "admin":
                    self._send(403, render_page("Forbidden", '<div class="es"><i class="fas fa-lock"></i><h3>Admin access required</h3></div>', user=user))
                else:
                    self._send(200, page_users(db, user))

            elif path == "/users/add":
                if user.get("role") != "admin":
                    self._send(403, render_page("Forbidden", '<div class="es"><i class="fas fa-lock"></i><h3>Admin access required</h3></div>', user=user))
                else:
                    self._send(200, page_add_user(nav_user=user))

            elif path.startswith("/users/") and path.endswith("/reset"):
                if user.get("role") != "admin":
                    self._send(403, render_page("Forbidden", '<div class="es"><i class="fas fa-lock"></i><h3>Admin access required</h3></div>', user=user))
                else:
                    try:
                        uid = int(path.split("/")[2])
                    except (ValueError, IndexError):
                        self._redirect("/users")
                        return
                    target = db.execute("SELECT username FROM users WHERE id=%s", (uid,)).fetchone()
                    if not target:
                        flash("User not found.", "error")
                        self._redirect("/users")
                    else:
                        self._send(200, page_reset_user_password(uid, target["username"], nav_user=user))

            elif path == "/logs":
                self._send(200, page_logs(db, nav_user=user))

            elif path.startswith("/api/track/"):
                # Legacy tracking pixel endpoint — kept alive so old emails don't
                # produce broken-image icons, but no longer updates applicant status.
                # Tracking pixels were removed from outgoing emails to improve
                # deliverability (cross-domain image loads trigger spam filters).
                gif = b'\x47\x49\x46\x38\x39\x61\x01\x00\x01\x00\x80\x00\x00\xff\xff\xff\x00\x00\x00\x21\xf9\x04\x01\x0a\x00\x01\x00\x2c\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02\x4d\x01\x00\x3b'
                self.send_response(200)
                self.send_header("Content-Type", "image/gif")
                self.send_header("Content-Length", str(len(gif)))
                self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
                self.end_headers()
                self.wfile.write(gif)

            elif path == "/api/debug-xml":
                # FIX: These debug endpoints were accessible without auth (all /api/ routes
                # bypassed auth). They expose raw PII from XML logs. Auth now required.
                row = db.execute("SELECT raw_xml FROM xml_log ORDER BY id DESC LIMIT 1").fetchone()
                if row and row["raw_xml"]:
                    self._send(200, row["raw_xml"], "text/xml; charset=utf-8")
                else:
                    self._send(200, "No XML logs found", "text/plain; charset=utf-8")

            elif path == "/api/debug-xml-tags":
                # FIX: Auth now required (same issue as above)
                row = db.execute("SELECT raw_xml FROM xml_log ORDER BY id DESC LIMIT 1").fetchone()
                if row and row["raw_xml"]:
                    try:
                        xroot = ET.fromstring(row["raw_xml"])
                        tags = set()
                        tag_tree = []
                        for el in xroot.iter():
                            tags.add(el.tag)
                            text_preview = (el.text or "").strip()[:50]
                            tag_tree.append(f"{el.tag} = '{text_preview}'" if text_preview else el.tag)
                        result = "=== ALL UNIQUE TAGS ===\n" + "\n".join(sorted(tags))
                        result += "\n\n=== FULL TAG TREE WITH VALUES ===\n" + "\n".join(tag_tree)
                        self._send(200, result, "text/plain; charset=utf-8")
                    except Exception as e:
                        self._send(200, f"Parse error: {e}", "text/plain; charset=utf-8")
                else:
                    self._send(200, "No XML logs found", "text/plain; charset=utf-8")

            else:
                self._send(404, render_page("Not Found",
                    '<div class="es"><i class="fas fa-exclamation-triangle"></i><h3>Page not found</h3></div>'))
        finally:
            db.close()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        db = get_db()

        try:
            # ---------------------------------------------------------------
            # Login (no session required)
            # ---------------------------------------------------------------
            if path == "/forgot-password":
                form_data = self._parse_form()
                def _fv_simple(name, default=""):
                    if isinstance(form_data, cgi.FieldStorage):
                        v = form_data.getfirst(name, default)
                        return v if isinstance(v, str) else (v.decode() if v else default)
                    vals = form_data.get(name, [default])
                    return vals[0] if vals else default
                identifier = _fv_simple("identifier").strip()
                if not identifier:
                    self._send(200, page_forgot_password(error="Please enter your username or recovery email."))
                else:
                    ok, msg = send_password_reset_email(db, identifier)
                    if ok:
                        self._send(200, page_forgot_password(success=msg))
                    else:
                        self._send(200, page_forgot_password(error=msg))
                return

            if path == "/reset-password":
                form_data = self._parse_form()
                def _fv_rp(name, default=""):
                    if isinstance(form_data, cgi.FieldStorage):
                        v = form_data.getfirst(name, default)
                        return v if isinstance(v, str) else (v.decode() if v else default)
                    vals = form_data.get(name, [default])
                    return vals[0] if vals else default
                token = _fv_rp("token").strip()
                new_pass = _fv_rp("password")
                confirm = _fv_rp("confirm")
                if not token:
                    self._redirect("/forgot-password")
                    return
                row = db.execute(
                    "SELECT * FROM password_reset_tokens WHERE token=%s AND used=FALSE AND expires_at > NOW()",
                    (token,)
                ).fetchone()
                if not row:
                    self._send(200, page_forgot_password(error="This reset link is invalid or has expired. Please request a new one."))
                    return
                if len(new_pass) < 8:
                    self._send(200, page_reset_password(token, error="Password must be at least 8 characters."))
                    return
                if new_pass != confirm:
                    self._send(200, page_reset_password(token, error="Passwords do not match."))
                    return
                pw_hash = hash_password(new_pass)
                db.execute("UPDATE users SET password_hash=%s WHERE id=%s", (pw_hash, row["user_id"]))
                db.execute("UPDATE password_reset_tokens SET used=TRUE WHERE token=%s", (token,))
                # Invalidate all active sessions for this user for security
                db.execute("DELETE FROM sessions WHERE user_id=%s", (row["user_id"],))
                logger.info(f"Password reset completed for user_id={row['user_id']}")
                flash("Password reset successfully. Please log in with your new password.", "success")
                self._redirect("/login")
                return

            if path == "/login":
                form_data = self._parse_form()

                def fv(name, default=""):
                    if isinstance(form_data, cgi.FieldStorage):
                        item = form_data.getfirst(name, default)
                        return item if isinstance(item, str) else (item.decode() if item else default)
                    else:
                        vals = form_data.get(name, [default])
                        return vals[0] if vals else default

                username = fv("username").strip()
                password = fv("password")

                user = db.execute(
                    "SELECT * FROM users WHERE username=%s AND is_active=TRUE",
                    (username,)
                ).fetchone()
                if user and verify_password(password, user["password_hash"]):
                    token = create_session(db, user["id"])
                    db.execute("UPDATE users SET last_login=NOW() WHERE id=%s", (user["id"],))
                    self._redirect("/", set_cookie=token)
                else:
                    self._send(200, page_login(error="Invalid username or password."))
                return

            # ---------------------------------------------------------------
            # Accio Data XML push (no session, but credential-authenticated)
            # ---------------------------------------------------------------
            if path == "/api/accio-push":
                try:
                    ACCIO_USERNAME = get_setting(db, "accio_username") or os.environ.get("ACCIO_USERNAME", "")
                    ACCIO_PASSWORD = get_setting(db, "accio_password") or os.environ.get("ACCIO_PASSWORD", "")

                    # FIX: Enforce maximum body size to prevent memory exhaustion
                    content_length = int(self.headers.get("Content-Length", 0))
                    if content_length > MAX_XML_BODY:
                        logger.warning(f"Accio push body too large: {content_length} bytes")
                        self._send(413, '<?xml version="1.0" encoding="UTF-8"?>\n<BackgroundReports><error>Request body too large</error></BackgroundReports>', "text/xml; charset=utf-8")
                        return

                    raw = self.rfile.read(content_length).decode("utf-8", errors="replace") if content_length > 0 else ""
                    auth_valid = False

                    # Method 1: HTTP Basic Auth header
                    auth_header = self.headers.get("Authorization", "")
                    if auth_header.startswith("Basic "):
                        try:
                            decoded = base64.b64decode(auth_header[6:]).decode()
                            u, p = decoded.split(":", 1)
                            if u == ACCIO_USERNAME and p == ACCIO_PASSWORD and ACCIO_PASSWORD:
                                auth_valid = True
                        except Exception:
                            pass

                    # Method 2: Query params
                    qs = urllib.parse.parse_qs(parsed.query)
                    if not auth_valid and ACCIO_PASSWORD:
                        if (qs.get("username", [None])[0] == ACCIO_USERNAME and
                                qs.get("password", [None])[0] == ACCIO_PASSWORD):
                            auth_valid = True

                    # Method 3: Credentials in XML body
                    if not auth_valid and raw.strip() and ACCIO_PASSWORD:
                        try:
                            auth_root = ET.fromstring(raw)
                            login_elem = auth_root.find("login")
                            if login_elem is not None:
                                xml_user = (login_elem.findtext("username") or "").strip()
                                xml_pass = (login_elem.findtext("password") or "").strip()
                                if xml_user == ACCIO_USERNAME and xml_pass == ACCIO_PASSWORD:
                                    auth_valid = True
                            if not auth_valid:
                                for parent in [auth_root] + list(auth_root):
                                    xml_user = xml_pass = None
                                    for el in parent.iter():
                                        tag = el.tag.lower() if el.tag else ""
                                        if tag in ("username", "user", "remote_username"):
                                            xml_user = (el.text or "").strip()
                                        elif tag in ("password", "pass", "remote_password"):
                                            xml_pass = (el.text or "").strip()
                                    if xml_user == ACCIO_USERNAME and xml_pass == ACCIO_PASSWORD:
                                        auth_valid = True
                                        break
                            if not auth_valid:
                                root_user = auth_root.get("username") or auth_root.get("user") or ""
                                root_pass = auth_root.get("password") or auth_root.get("pass") or ""
                                if root_user == ACCIO_USERNAME and root_pass == ACCIO_PASSWORD:
                                    auth_valid = True
                        except ET.ParseError:
                            logging.warning("XML parse error during auth check")
                        except Exception as e:
                            logging.warning(f"Unexpected error during XML auth check: {e}")

                    # Method 4: Custom HTTP headers
                    if not auth_valid and ACCIO_PASSWORD:
                        h_user = self.headers.get("X-Username") or self.headers.get("Username") or ""
                        h_pass = self.headers.get("X-Password") or self.headers.get("Password") or ""
                        if h_user == ACCIO_USERNAME and h_pass == ACCIO_PASSWORD:
                            auth_valid = True

                    logging.info(f"Accio push auth: valid={auth_valid}, has_basic={bool(auth_header)}, body_len={len(raw)}")

                    if not auth_valid:
                        all_headers = {k: v for k, v in self.headers.items() if k.lower() != "authorization"}
                        logging.warning(f"Auth failed. Headers: {all_headers}")
                        logging.warning(f"Auth failed. Body preview: {raw[:200]}")
                        db.execute(
                            "INSERT INTO xml_log (direction,raw_xml,parsed_status,error_message) VALUES ('inbound',%s,'auth_failed','Authentication failed')",
                            (raw[:10000],)
                        )
                        self._send(401, '<?xml version="1.0" encoding="UTF-8"?>\n<BackgroundReports><error>Authentication required</error></BackgroundReports>', "text/xml; charset=utf-8")
                        return

                    logging.info(f"Accio push auth PASSED. Processing XML ({len(raw)} bytes)...")
                    try:
                        debug_root = ET.fromstring(raw)
                        for el in debug_root.iter():
                            txt = (el.text or "").strip()
                            if txt:
                                logging.info(f"  XML TAG: <{el.tag}> = '{txt[:80]}'")
                            else:
                                logging.info(f"  XML TAG: <{el.tag}>")
                    except Exception:
                        pass

                    db.execute(
                        "INSERT INTO xml_log (direction,raw_xml,parsed_status) VALUES ('inbound',%s,'processing')",
                        (raw[:10000],)
                    )

                    applicants_data, err = parse_accio_xml(raw)
                    if err:
                        logging.error(f"XML parse error: {err}")
                        db.execute(
                            "UPDATE xml_log SET parsed_status='error',error_message=%s WHERE id=(SELECT MAX(id) FROM xml_log)",
                            (err,)
                        )
                        self._send(400, '<?xml version="1.0" encoding="UTF-8"?>\n<BackgroundReports><error>XML parse error</error></BackgroundReports>', "text/xml; charset=utf-8")
                        return

                    added = 0
                    auto_assign = get_setting(db, "auto_assign_codes") == "1"
                    auto_email = get_setting(db, "auto_send_email") == "1"
                    auto_sms = get_setting(db, "auto_send_sms") == "1"

                    for a in applicants_data:
                        try:
                            ex = (db.execute("SELECT id FROM applicants WHERE accio_order_number = %s",
                                             (a["accio_order_number"],)).fetchone()
                                  if a.get("accio_order_number") else None)
                            if not ex:
                                client_id = None
                                if a.get("company_name"):
                                    client = db.execute(
                                        "SELECT id FROM clients WHERE company_name=%s",
                                        (a["company_name"],)
                                    ).fetchone()
                                    if not client:
                                        cur = db.execute(
                                            "INSERT INTO clients (company_name, account_name) VALUES (%s, %s) RETURNING id",
                                            (a["company_name"], a.get("account_name", ""))
                                        )
                                        client_id = cur.fetchone()["id"]
                                    else:
                                        client_id = client["id"]

                                cur = db.execute(
                                    "INSERT INTO applicants (first_name,last_name,email,phone,accio_order_number,accio_remote_number,client_id,accio_sub_order,accio_order_type,date_of_birth,last_four_ssn) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                                    (a["first_name"], a["last_name"], a.get("email", ""),
                                     a.get("phone", ""), a.get("accio_order_number", ""),
                                     a.get("accio_remote_number", ""), client_id,
                                     a.get("accio_sub_order", "1"), a.get("accio_order_type", "Fingerprint"),
                                     a.get("date_of_birth", ""), a.get("last_four_ssn", ""))
                                )
                                new_id = cur.fetchone()["id"]
                                added += 1

                                if auto_assign:
                                    code_row = db.execute(
                                        "SELECT id, code FROM codes WHERE assigned_to IS NULL ORDER BY id LIMIT 1"
                                    ).fetchone()
                                    if code_row:
                                        db.execute(
                                            "UPDATE codes SET assigned_to=%s, assigned_date=NOW() WHERE id = %s",
                                            (new_id, code_row["id"])
                                        )
                                        db.execute(
                                            "UPDATE applicants SET assigned_code=%s, status='code_assigned' WHERE id = %s",
                                            (code_row["code"], new_id)
                                        )
                                        # Only auto-email if applicant has a valid email address
                                        if auto_email and a.get("email", "").strip():
                                            try:
                                                send_release_email(db, new_id)
                                            except Exception as email_err:
                                                logging.error(f"Auto-email failed for applicant {new_id}: {email_err}")
                                        # Auto-SMS if applicant has a valid phone number
                                        if auto_sms and a.get("phone", "").strip():
                                            try:
                                                send_release_sms(db, new_id)
                                            except Exception as sms_err:
                                                logging.error(f"Auto-SMS failed for applicant {new_id}: {sms_err}")
                        except Exception as proc_err:
                            logging.error(f"Error processing applicant: {proc_err}")

                    db.execute(
                        "UPDATE xml_log SET parsed_status='success',error_message=%s WHERE id=(SELECT MAX(id) FROM xml_log)",
                        (f"Added {added} applicants from {len(applicants_data)} parsed",)
                    )
                    logging.info(f"Accio push complete: {added} added from {len(applicants_data)} parsed")

                    resp_xml = ('<?xml version="1.0" encoding="UTF-8"?>\n'
                                '<BackgroundReports>\n'
                                '  <BackgroundReportPackage>\n'
                                f'    <ScreeningStatus>accepted</ScreeningStatus>\n'
                                f'    <ResultsRetrieved>{added}</ResultsRetrieved>\n'
                                '  </BackgroundReportPackage>\n'
                                '</BackgroundReports>')
                    self._send(200, resp_xml, "text/xml; charset=utf-8")
                    return

                except Exception as e:
                    logging.error(f"CRITICAL: Unhandled exception in accio-push: {e}", exc_info=True)
                    try:
                        db.execute(
                            "INSERT INTO xml_log (direction,raw_xml,parsed_status,error_message) VALUES ('inbound','','crash',%s)",
                            (str(e)[:5000],)
                        )
                    except Exception:
                        pass
                    self._send(500, '<?xml version="1.0" encoding="UTF-8"?>\n<BackgroundReports><error>Internal server error</error></BackgroundReports>', "text/xml; charset=utf-8")
                    return

            # ---------------------------------------------------------------
            # All other POST routes require session auth
            # ---------------------------------------------------------------
            user = self._check_auth()
            if not user:
                self._redirect("/login")
                return

            # Bulk code upload (JSON)
            if path == "/api/codes":
                length = int(self.headers.get("Content-Length", 0))
                raw = self.rfile.read(length).decode("utf-8", errors="replace")
                try:
                    data = json.loads(raw)
                    codes_list = data.get("codes", [])
                    batch_name = data.get("batch_name", f"API Import {datetime.now().strftime('%Y-%m-%d %H:%M')}")
                    if not codes_list:
                        self._send(400, json.dumps({"status": "error", "message": 'No codes provided. Send {"codes": ["CODE1", ...]}'}), "application/json")
                        return
                    imported, dups = 0, 0
                    for code in codes_list:
                        code = str(code).strip()
                        if code:
                            try:
                                db.execute("INSERT INTO codes (code, batch_name) VALUES (%s, %s)", (code, batch_name))
                                imported += 1
                            except psycopg2.IntegrityError:
                                db.rollback()
                                dups += 1
                    self._send(200, json.dumps({"status": "success", "imported": imported, "duplicates": dups, "batch": batch_name}), "application/json")
                except json.JSONDecodeError:
                    self._send(400, json.dumps({"status": "error", "message": "Invalid JSON"}), "application/json")
                except Exception as e:
                    self._send(500, json.dumps({"status": "error", "message": str(e)}), "application/json")
                return

            # Bulk code upload from file
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
                        if os.path.exists(fpath):
                            os.remove(fpath)
                else:
                    self._send(400, json.dumps({"status": "error", "message": "Send file as multipart form with field name 'file'"}), "application/json")
                return

            # General form handler
            form_data = self._parse_form()

            def fv(name, default=""):
                if isinstance(form_data, cgi.FieldStorage):
                    item = form_data.getfirst(name, default)
                    return item if isinstance(item, str) else (item.decode() if item else default)
                else:
                    vals = form_data.get(name, [default])
                    return vals[0] if vals else default

            if path == "/applicants/add":
                db.execute(
                    "INSERT INTO applicants (first_name,last_name,email,phone,accio_order_number,notes,date_of_birth,last_four_ssn) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (fv("first_name"), fv("last_name"), fv("email"), fv("phone"),
                     fv("accio_order_number"), fv("notes"), fv("date_of_birth"), fv("last_four_ssn"))
                )
                flash("Applicant added.", "success")
                self._redirect("/applicants")

            elif path.startswith("/applicants/") and path.endswith("/assign-code"):
                aid = int(path.split("/")[2])
                code_val, msg = assign_code(db, aid)
                flash(f"Code {code_val} assigned." if code_val else f"Failed: {msg}",
                      "success" if code_val else "error")
                self._redirect("/applicants")

            elif path.startswith("/applicants/") and path.endswith("/send-email"):
                aid = int(path.split("/")[2])
                ok, msg = send_release_email(db, aid)
                # Also send SMS if auto_send_sms is on and applicant has phone
                sms_msg = ""
                a_row = db.execute("SELECT phone FROM applicants WHERE id = %s", (aid,)).fetchone()
                if get_setting(db, "auto_send_sms") == "1" and a_row and a_row.get("phone", "").strip():
                    try:
                        sms_ok, sms_msg = send_release_sms(db, aid)
                        sms_msg = " SMS also sent!" if sms_ok else f" SMS failed: {sms_msg}"
                    except Exception as sms_err:
                        sms_msg = f" SMS error: {sms_err}"
                flash(msg + sms_msg, "success" if ok else "error")
                self._redirect("/applicants")

            elif path.startswith("/applicants/") and path.endswith("/assign-and-send"):
                aid = int(path.split("/")[2])
                a = db.execute("SELECT * FROM applicants WHERE id = %s", (aid,)).fetchone()
                if a and not a["assigned_code"]:
                    assign_code(db, aid)
                ok, msg = send_release_email(db, aid)
                # Also send SMS
                sms_note = ""
                a_refresh = db.execute("SELECT phone FROM applicants WHERE id = %s", (aid,)).fetchone()
                if get_setting(db, "auto_send_sms") == "1" and a_refresh and a_refresh.get("phone", "").strip():
                    try:
                        sms_ok, sms_detail = send_release_sms(db, aid)
                        sms_note = " + SMS sent!" if sms_ok else f" (SMS failed: {sms_detail})"
                    except Exception as sms_err:
                        sms_note = f" (SMS error: {sms_err})"
                flash(("Code assigned & email sent!" + sms_note) if ok else f"Code assigned but email failed: {msg}",
                      "success" if ok else "error")
                self._redirect("/applicants")

            elif path.startswith("/applicants/") and path.endswith("/send-sms"):
                aid = int(path.split("/")[2])
                ok, msg = send_release_sms(db, aid)
                flash(msg, "success" if ok else "error")
                self._redirect("/applicants")

            elif path.startswith("/applicants/") and path.endswith("/update-email"):
                aid = int(path.split("/")[2])
                new_email = fv("email").strip()
                # FIX: Added basic email format validation
                if new_email and "@" in new_email and "." in new_email.split("@")[-1]:
                    db.execute("UPDATE applicants SET email = %s, updated_at = NOW() WHERE id = %s",
                               (new_email, aid))
                    flash(f"Email updated to {new_email}", "success")
                elif not new_email:
                    flash("Email cannot be blank.", "error")
                else:
                    flash("Invalid email address format.", "error")
                self._redirect("/applicants")

            elif path.startswith("/applicants/") and path.endswith("/resend"):
                aid = int(path.split("/")[2])
                a = db.execute("SELECT * FROM applicants WHERE id = %s", (aid,)).fetchone()
                if not a:
                    flash("Applicant not found.", "error")
                elif not a["email"]:
                    flash("No email address on file. Update the email first.", "error")
                elif not a["assigned_code"]:
                    flash("No code assigned yet. Use 'Assign & Send' first.", "error")
                else:
                    ok, msg = send_release_email(db, aid)
                    # Also resend SMS if phone is available
                    sms_note = ""
                    if get_setting(db, "auto_send_sms") == "1" and a.get("phone", "").strip():
                        try:
                            sms_ok, sms_detail = send_release_sms(db, aid)
                            sms_note = f" SMS also resent to {a['phone']}!" if sms_ok else f" (SMS resend failed: {sms_detail})"
                        except Exception as sms_err:
                            sms_note = f" (SMS error: {sms_err})"
                    flash((f"Email resent to {a['email']}!" + sms_note) if ok else f"Resend failed: {msg}",
                          "success" if ok else "error")
                self._redirect("/applicants")

            elif path.startswith("/applicants/") and path.endswith("/mark-complete"):
                aid = int(path.split("/")[2])
                db.execute("UPDATE applicants SET status='completed' WHERE id = %s", (aid,))
                flash("Applicant marked complete.", "success")
                self._redirect("/applicants")

            elif path.startswith("/applicants/") and path.endswith("/delete"):
                aid = int(path.split("/")[2])
                applicant = db.execute("SELECT assigned_code FROM applicants WHERE id = %s", (aid,)).fetchone()
                if applicant and applicant["assigned_code"]:
                    db.execute("UPDATE codes SET assigned_to=NULL, assigned_date=NULL WHERE code = %s",
                               (applicant["assigned_code"],))
                db.execute("DELETE FROM email_tracking WHERE applicant_id = %s", (aid,))
                db.execute("DELETE FROM email_log WHERE applicant_id = %s", (aid,))
                db.execute("DELETE FROM sms_log WHERE applicant_id = %s", (aid,))
                db.execute("DELETE FROM applicants WHERE id = %s", (aid,))
                flash("Applicant deleted.", "success")
                self._redirect("/applicants")

            elif path == "/applicants/bulk-process":
                pending = db.execute(
                    "SELECT * FROM applicants WHERE status='pending' AND email IS NOT NULL AND email != ''"
                ).fetchall()
                succ, fail, sms_succ, sms_fail = 0, 0, 0, 0
                bulk_auto_sms = get_setting(db, "auto_send_sms") == "1"
                for i, a in enumerate(pending):
                    # Throttle: 2-second delay between messages to avoid being
                    # flagged as a spam bot by receiving mail servers.
                    if i > 0:
                        time.sleep(2)
                    c_val, _ = assign_code(db, a["id"])
                    if c_val:
                        ok, _ = send_release_email(db, a["id"])
                        if ok:
                            succ += 1
                        else:
                            fail += 1
                        # Also send SMS if phone available
                        if bulk_auto_sms and a.get("phone", "").strip():
                            try:
                                s_ok, _ = send_release_sms(db, a["id"])
                                if s_ok:
                                    sms_succ += 1
                                else:
                                    sms_fail += 1
                            except Exception:
                                sms_fail += 1
                    else:
                        fail += 1
                        break  # Stop if we run out of codes
                sms_note = f" | SMS: {sms_succ} sent, {sms_fail} failed" if (sms_succ + sms_fail) > 0 else ""
                flash(f"Done: {succ} emailed, {fail} failed{sms_note}.", "success")
                self._redirect("/applicants")

            elif path.startswith("/codes/") and path.endswith("/delete"):
                code_id = int(path.split("/")[2])
                code_row = db.execute("SELECT * FROM codes WHERE id = %s", (code_id,)).fetchone()
                if not code_row:
                    flash("Code not found.", "error")
                elif code_row["assigned_to"]:
                    flash("Cannot delete a code that is already assigned to an applicant.", "error")
                else:
                    db.execute("DELETE FROM codes WHERE id = %s", (code_id,))
                    flash(f"Code '{code_row['code']}' deleted.", "success")
                self._redirect("/codes")

            elif path == "/codes/add-manual":
                codes_text = fv("codes")
                batch = fv("batch_name", "Manual")
                imp, dup = 0, 0
                for line in codes_text.strip().split("\n"):
                    code_str = line.strip()
                    if code_str:
                        try:
                            db.execute("INSERT INTO codes (code,batch_name) VALUES (%s,%s)",
                                       (code_str, batch))
                            imp += 1
                        except psycopg2.IntegrityError:
                            # FIX: db.rollback() now works — rollback() method added to DBHelper
                            db.rollback()
                            dup += 1
                flash(f"Added {imp} codes ({dup} duplicates skipped).", "success")
                self._redirect("/codes")

            elif path == "/codes/import":
                if isinstance(form_data, cgi.FieldStorage):
                    file_item = form_data["file"]
                    col_idx = int(form_data.getfirst("column_index", "0"))
                    skip_h = form_data.getfirst("skip_header") == "on"
                    batch = form_data.getfirst("batch_name", "Import")
                    fname = file_item.filename or "upload"
                    fpath = os.path.join(UPLOAD_FOLDER, f"import_{datetime.now().strftime('%Y%m%d%H%M%S')}_{fname}")
                    with open(fpath, "wb") as f:
                        f.write(file_item.file.read())
                    imp, dup, err = import_codes_from_file(fpath, column_index=col_idx,
                                                           skip_header=skip_h, batch_name=batch)
                    if err:
                        flash(f"Import error: {err}", "error")
                    else:
                        flash(f"Imported {imp} codes ({dup} duplicates) from '{batch}'.", "success")
                    if os.path.exists(fpath):
                        os.remove(fpath)
                self._redirect("/codes")

            elif path == "/settings":
                if isinstance(form_data, cgi.FieldStorage):
                    for key in DEFAULT_SETTINGS:
                        vals = form_data.getlist(key)
                        if vals:
                            set_setting(db, key, vals[-1])
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
                            set_setting(db, key, vals[-1])
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
                        logger.info(f"Test email: Connecting to {srv_host}:{srv_port} (TLS={use_tls})")
                        srv = smtplib.SMTP(srv_host, srv_port, timeout=15)
                        srv.set_debuglevel(1)
                        if use_tls:
                            srv.starttls()
                        if srv_user and srv_pass:
                            srv.login(srv_user, srv_pass)
                        srv.send_message(msg)
                        srv.quit()
                        logger.info(f"Test email sent successfully to {addr}")
                        flash(f"Test email sent to {addr}!", "success")
                    except Exception as e:
                        logger.error(f"Test email FAILED: {type(e).__name__}: {e}")
                        flash(f"Test failed: {type(e).__name__}: {e}", "error")
                self._redirect("/settings")

            elif path == "/settings/test-sms":
                phone = fv("test_phone").strip()
                if not phone:
                    flash("Enter a phone number to test.", "error")
                elif not HAS_TWILIO:
                    flash("Twilio library not installed. Run: pip install twilio", "error")
                else:
                    normalized = normalize_phone(phone)
                    if not normalized:
                        flash(f"Invalid phone number: {phone}. Use 10-digit US format.", "error")
                    else:
                        try:
                            sid = get_setting(db, "twilio_account_sid").strip()
                            tok = get_setting(db, "twilio_auth_token").strip()
                            frm = get_setting(db, "twilio_from_number").strip()
                            if not sid or not tok or not frm:
                                flash("Configure Twilio SID, Auth Token, and From Number first.", "error")
                            else:
                                client = TwilioClient(sid, tok)
                                msg = client.messages.create(
                                    body="Test SMS from Fingerprint Release Manager. Twilio is working!",
                                    from_=frm,
                                    to=normalized
                                )
                                logger.info(f"Test SMS sent to {normalized}, SID={msg.sid}")
                                flash(f"Test SMS sent to {normalized}! (SID: {msg.sid})", "success")
                        except Exception as e:
                            logger.error(f"Test SMS FAILED: {type(e).__name__}: {e}")
                            flash(f"Test SMS failed: {type(e).__name__}: {e}", "error")
                self._redirect("/settings")

            # ------------------------------------------------------------------
            # Profile / My Account
            # ------------------------------------------------------------------
            elif path == "/profile":
                new_display = fv("display_name").strip()
                new_recovery = fv("recovery_email").strip()
                if new_recovery and ("@" not in new_recovery or "." not in new_recovery.split("@")[-1]):
                    flash("Invalid recovery email format.", "error")
                else:
                    db.execute(
                        "UPDATE users SET display_name=%s, recovery_email=%s WHERE id=%s",
                        (new_display or None, new_recovery or None, user["id"])
                    )
                    flash("Profile updated.", "success")
                self._redirect("/profile")

            elif path == "/profile/change-password":
                current = fv("current_password")
                new_pw = fv("new_password")
                confirm = fv("confirm_password")
                u_row = db.execute("SELECT * FROM users WHERE id=%s", (user["id"],)).fetchone()
                if not verify_password(current, u_row["password_hash"]):
                    flash("Current password is incorrect.", "error")
                elif len(new_pw) < 8:
                    flash("New password must be at least 8 characters.", "error")
                elif new_pw != confirm:
                    flash("New passwords do not match.", "error")
                else:
                    db.execute("UPDATE users SET password_hash=%s WHERE id=%s",
                               (hash_password(new_pw), user["id"]))
                    flash("Password changed successfully.", "success")
                self._redirect("/profile")

            # ------------------------------------------------------------------
            # User Management (admin only)
            # ------------------------------------------------------------------
            elif path == "/users/add":
                if user.get("role") != "admin":
                    self._send(403, render_page("Forbidden", '<div class="es"><i class="fas fa-lock"></i><h3>Admin access required</h3></div>', user=user))
                else:
                    new_username = fv("username").strip()
                    new_display = fv("display_name").strip()
                    new_password = fv("password")
                    new_role = fv("role", "user")
                    new_recovery = fv("recovery_email").strip()
                    if not new_username or not new_password:
                        flash("Username and password are required.", "error")
                        self._redirect("/users/add")
                    elif len(new_password) < 8:
                        flash("Password must be at least 8 characters.", "error")
                        self._redirect("/users/add")
                    elif new_role not in ("user", "admin"):
                        flash("Invalid role.", "error")
                        self._redirect("/users/add")
                    else:
                        try:
                            db.execute(
                                "INSERT INTO users (username, password_hash, display_name, role, recovery_email, is_active) VALUES (%s,%s,%s,%s,%s,TRUE)",
                                (new_username, hash_password(new_password), new_display or None, new_role, new_recovery or None)
                            )
                            flash(f"User '{new_username}' created.", "success")
                            self._redirect("/users")
                        except psycopg2.IntegrityError:
                            db.rollback()
                            flash(f"Username '{new_username}' already exists.", "error")
                            self._redirect("/users/add")

            elif path.startswith("/users/") and path.endswith("/toggle"):
                if user.get("role") != "admin":
                    self._send(403, render_page("Forbidden", '<div class="es"><i class="fas fa-lock"></i><h3>Admin access required</h3></div>', user=user))
                else:
                    try:
                        uid = int(path.split("/")[2])
                    except (ValueError, IndexError):
                        self._redirect("/users")
                        return
                    if uid == user["id"]:
                        flash("You cannot disable your own account.", "error")
                    else:
                        target = db.execute("SELECT is_active, username FROM users WHERE id=%s", (uid,)).fetchone()
                        if target:
                            new_state = not target["is_active"]
                            db.execute("UPDATE users SET is_active=%s WHERE id=%s", (new_state, uid))
                            action = "enabled" if new_state else "disabled"
                            flash(f"User '{target['username']}' {action}.", "success")
                    self._redirect("/users")

            elif path.startswith("/users/") and path.endswith("/reset"):
                if user.get("role") != "admin":
                    self._send(403, render_page("Forbidden", '<div class="es"><i class="fas fa-lock"></i><h3>Admin access required</h3></div>', user=user))
                else:
                    try:
                        uid = int(path.split("/")[2])
                    except (ValueError, IndexError):
                        self._redirect("/users")
                        return
                    new_pw = fv("new_password")
                    confirm = fv("confirm_password")
                    target = db.execute("SELECT username FROM users WHERE id=%s", (uid,)).fetchone()
                    if not target:
                        flash("User not found.", "error")
                        self._redirect("/users")
                    elif len(new_pw) < 8:
                        self._send(200, page_reset_user_password(uid, target["username"], nav_user=user, error="Password must be at least 8 characters."))
                    elif new_pw != confirm:
                        self._send(200, page_reset_user_password(uid, target["username"], nav_user=user, error="Passwords do not match."))
                    else:
                        db.execute("UPDATE users SET password_hash=%s WHERE id=%s", (hash_password(new_pw), uid))
                        # Invalidate existing sessions for that user
                        db.execute("DELETE FROM sessions WHERE user_id=%s", (uid,))
                        flash(f"Password reset for '{target['username']}'.", "success")
                        self._redirect("/users")

            elif path.startswith("/users/") and path.endswith("/delete"):
                if user.get("role") != "admin":
                    self._send(403, render_page("Forbidden", '<div class="es"><i class="fas fa-lock"></i><h3>Admin access required</h3></div>', user=user))
                else:
                    try:
                        uid = int(path.split("/")[2])
                    except (ValueError, IndexError):
                        self._redirect("/users")
                        return
                    target = db.execute("SELECT username, role FROM users WHERE id=%s", (uid,)).fetchone()
                    if not target:
                        flash("User not found.", "error")
                    elif target["role"] == "admin":
                        flash("Admin accounts cannot be deleted.", "error")
                    elif uid == user["id"]:
                        flash("You cannot delete your own account.", "error")
                    else:
                        db.execute("DELETE FROM sessions WHERE user_id=%s", (uid,))
                        db.execute("DELETE FROM password_reset_tokens WHERE user_id=%s", (uid,))
                        db.execute("DELETE FROM users WHERE id=%s", (uid,))
                        flash(f"User '{target['username']}' deleted.", "success")
                    self._redirect("/users")

            else:
                self._send(404, render_page("Not Found",
                    '<div class="es"><i class="fas fa-exclamation-triangle"></i><h3>Page not found</h3></div>'))

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
    ║   Fingerprint Release Manager v2.0                       ║
    ║   Web UI:      http://localhost:{PORT}                     ║
    ║                                                          ║
    ║   LOGIN:                                                 ║
    ║   Credentials loaded from environment variables          ║
    ║   (DEFAULT_ADMIN_USER / DEFAULT_ADMIN_PASSWORD)          ║
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
