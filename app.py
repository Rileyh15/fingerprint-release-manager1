"""
Fingerprint Release Manager - v2.0
Integrates with Accio Data XML API to automate fingerprint release form distribution.

New Features:
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
5. Tracks email opens and applicant status

Built with Python standard library + openpyxl for Excel support.
"""

import os
import sys
import json
import smtplib
import logging
import urllib.parse
import cgi
import io
import csv
import shutil
import base64
import uuid
import hashlib
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from xml.etree import ElementTree as ET
from string import Template

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

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_URL = os.environ.get("DATABASE_URL", "")
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

    def close(self):
        self._conn.close()

def get_db():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    conn.autocommit = True
    return DBHelper(conn)

def init_db():
    db = get_db()
    cur = db.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    cur = db.execute("""
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
    cur = db.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            created_at TIMESTAMP DEFAULT NOW(),
            expires_at TIMESTAMP
        )
    """)
    cur = db.execute("""
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
    cur = db.execute("""
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
            client_id INTEGER REFERENCES clients(id)
        )
    """)
    cur = db.execute("""
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
    cur = db.execute("""
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
    cur = db.execute("""
        CREATE TABLE IF NOT EXISTS email_tracking (
            id SERIAL PRIMARY KEY,
            applicant_id INTEGER REFERENCES applicants(id),
            email_log_id INTEGER REFERENCES email_log(id),
            tracking_token TEXT UNIQUE NOT NULL,
            opened_at TIMESTAMP,
            open_count INTEGER DEFAULT 0,
            user_agent TEXT
        )
    """)
    cur = db.execute("""
        CREATE TABLE IF NOT EXISTS xml_log (
            id SERIAL PRIMARY KEY,
            direction TEXT,
            raw_xml TEXT,
            parsed_status TEXT,
            error_message TEXT,
            received_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # Add new columns to existing tables if they don't exist
    try:
        db.execute("ALTER TABLE applicants ADD COLUMN client_id INTEGER REFERENCES clients(id)")
    except psycopg2.Error:
        pass  # Column already exists

    db.close()

    # Create default admin user if no users exist
    db = get_db()
    users = db.execute("SELECT COUNT(*) as cnt FROM users").fetchone()
    if users["cnt"] == 0:
        salt = os.urandom(32)
        admin_hash = hashlib.sha256(b"admin123" + salt).hexdigest()
        salt_b64 = base64.b64encode(salt).decode()
        password_with_salt = f"{admin_hash}${salt_b64}"
        db.execute(
            "INSERT INTO users (username, password_hash, display_name, role, is_active) VALUES (%s, %s, %s, %s, %s)",
            ("admin", password_with_salt, "Administrator", "admin", True)
        )
        logger.info("Created default admin user: admin / admin123")
    db.close()

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
DEFAULT_SETTINGS = {
    "accio_account": os.environ.get("ACCIO_ACCOUNT", "brsolutions"),
    "accio_username": os.environ.get("ACCIO_USERNAME", "admin"),
    "accio_password": os.environ.get("ACCIO_PASSWORD", ""),
    "accio_post_url": os.environ.get("ACCIO_POST_URL", "https://fingerprint-release-manager1.onrender.com/api/accio-push"),
    "smtp_server": os.environ.get("SMTP_HOST", "smtp.office365.com"),
    "smtp_port": os.environ.get("SMTP_PORT", "587"),
    "smtp_username": os.environ.get("SMTP_USER", ""),
    "smtp_password": os.environ.get("SMTP_PASS", ""),
    "smtp_use_tls": os.environ.get("SMTP_USE_TLS", "1"),
    "sender_email": os.environ.get("SENDER_EMAIL", "apply2check@br-solutions.net"),
    "sender_name": "Fingerprints Required",
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
    "company_name": "Background Research Solutions, LLC",
    "ori_number": "",
    "auto_assign_codes": "1",
    "auto_send_email": "1",
}

def get_setting(db, key):
    cur = db.execute("SELECT value FROM settings WHERE key = %s", (key,))
    row = cur.fetchone()
    if row:
        return row["value"]
    return DEFAULT_SETTINGS.get(key, "")

def set_setting(db, key, value):
    cur = db.execute("INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", (key, value))

# ---------------------------------------------------------------------------
# Authentication & Session Management
# ---------------------------------------------------------------------------
def hash_password(password, salt=None):
    """Hash a password with salt (sha256)."""
    if salt is None:
        salt = os.urandom(32)
    else:
        salt = base64.b64decode(salt)
    h = hashlib.sha256(password.encode() + salt).hexdigest()
    salt_b64 = base64.b64encode(salt).decode()
    return f"{h}${salt_b64}"

def verify_password(password, password_with_salt):
    """Verify a password against a hash."""
    try:
        h, salt_b64 = password_with_salt.split("$", 1)
        salt = base64.b64decode(salt_b64)
        computed = hashlib.sha256(password.encode() + salt).hexdigest()
        return computed == h
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
        "SELECT u.* FROM users u JOIN sessions s ON u.id = s.user_id WHERE s.token = %s AND s.expires_at > NOW()",
        (token,)
    )
    return cur.fetchone()

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
        for subject in complete_order.iter("subject"):
            first = _xt(subject, "name_first")
            last = _xt(subject, "name_last")
            email = _xt(subject, "email")
            phone = _xt(subject, "phone")
            if first or last:
                applicants.append(dict(first_name=first, last_name=last, email=email,
                                       phone=phone, accio_order_number=order_number,
                                       accio_remote_number=remote_number, company_name=""))

    for po in root.iter("placeOrder"):
        # Get company name from accountInfo
        company_name = ""
        ai = po.find("accountInfo")
        if ai is not None:
            company_name = _xt(ai, "company_name")
        # Get account from clientInfo
        account_name = ""
        ci = po.find("clientInfo")
        if ci is not None:
            account_name = _xt(ci, "account")

        # Get email/phone from orderInfo (Accio puts contact info here, not in subject)
        oi = po.find("orderInfo")
        oi_email = ""
        oi_phone = ""
        if oi is not None:
            oi_email = _xt(oi, "requester_email")
            oi_phone = _xt(oi, "requester_phone")
        # Fallback to clientInfo
        if not oi_email:
            if ci is not None:
                oi_email = _xt(ci, "primaryuser_contact_email")
                if not oi_phone:
                    oi_phone = _xt(ci, "primaryuser_contact_telephone")
        # Fallback to accountInfo
        if not oi_email:
            if ai is not None:
                oi_email = _xt(ai, "primaryuser_contact_email")
                if not oi_phone:
                    oi_phone = _xt(ai, "primaryuser_contact_telephone")
        for subject in po.iter("subject"):
            first = _xt(subject, "name_first")
            last = _xt(subject, "name_last")
            email = _xt(subject, "email") or oi_email
            phone = _xt(subject, "phone") or oi_phone
            if first or last:
                applicants.append(dict(first_name=first, last_name=last, email=email,
                                       phone=phone, accio_order_number=po.get("number", ""),
                                       accio_remote_number="", company_name=company_name, account_name=account_name))

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
                                       accio_remote_number=remote_order, company_name="", account_name=""))

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
                                       accio_remote_number=remote_number, company_name="", account_name=""))

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
                                       accio_remote_number="", company_name="", account_name=""))

    # --- Format 5: AccioOrder XML format (placeOrder > subject) ---
    # Accio sends: <AccioOrder><placeOrder number="..."><orderInfo><requester_email>...
    #              <subject><name_first>...<name_last>... (NO email/phone in subject!)
    # Email and phone are in <orderInfo> or <clientInfo> siblings, not in <subject>
    if not applicants:
        for place_order in root.iter("placeOrder"):
            order_number = place_order.get("number", "")
            # Get company name
            company_name = ""
            ai = place_order.find("accountInfo")
            if ai is not None:
                company_name = _xt(ai, "company_name")
            # Get account name
            account_name = ""
            ci = place_order.find("clientInfo")
            if ci is not None:
                account_name = _xt(ci, "account")

            # Get email/phone from orderInfo (sibling of subject within placeOrder)
            order_info = place_order.find("orderInfo")
            order_email = ""
            order_phone = ""
            if order_info is not None:
                order_email = (_xt(order_info, "requester_email") or
                              _xt(order_info, "requester_fax") or "")
                order_phone = (_xt(order_info, "requester_phone") or "")
            # Fallback: try clientInfo or accountInfo
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
            for subject in place_order.iter("subject"):
                first = _xt(subject, "name_first")
                last = _xt(subject, "name_last")
                # Check subject first for email/phone (in case future XML includes them)
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
                # If not found in subject, use orderInfo values
                if not email:
                    email = order_email
                if not phone:
                    phone = order_phone
                if first or last:
                    applicants.append(dict(first_name=first, last_name=last, email=email,
                                           phone=phone, accio_order_number=order_number,
                                           accio_remote_number="", company_name=company_name, account_name=account_name))

    # --- Format 5b: Also try <order> tag (older AccioOrder variants) ---
    if not applicants:
        for order in root.iter("order"):
            order_number = order.get("number", "")
            remote_number = order.get("remote_order", "")
            order_info = order.find("orderInfo")
            order_email = ""
            order_phone = ""
            if order_info is not None:
                order_email = _xt(order_info, "requester_email")
                order_phone = _xt(order_info, "requester_phone")
            for subject in order.iter("subject"):
                first = _xt(subject, "name_first")
                last = _xt(subject, "name_last")
                email = (_xt(subject, "email") or _xt(subject, "Email") or
                        _xt(subject, "InternetEmailAddress") or _xt(subject, "email_address") or
                        _xt(subject, "contact_email"))
                phone = (_xt(subject, "phone") or _xt(subject, "Phone") or
                        _xt(subject, "phone_number") or _xt(subject, "FormattedNumber") or
                        _xt(subject, "contact_phone"))
                if not email:
                    email = order_email
                if not phone:
                    phone = order_phone
                if first or last:
                    applicants.append(dict(first_name=first, last_name=last, email=email,
                                           phone=phone, accio_order_number=order_number,
                                           accio_remote_number=remote_number, company_name="", account_name=""))

    # --- Deep scan fallback: look for name_first/name_last pairs anywhere ---
    if not applicants:
        def deep_scan_for_applicants(elem, parent_map=None):
            found = []
            if parent_map is None:
                parent_map = {c: p for p in elem.iter() for c in p}

            for el in elem.iter():
                if el.tag == "name_first" and el.text:
                    first = el.text.strip()
                    # Look for nearby name_last in same parent
                    parent = parent_map.get(el)
                    if parent is not None:
                        last = _xt(parent, "name_last")
                        email = ""
                        phone = ""
                        # Try to find email and phone in parent or siblings
                        for tag_variant in ["email", "Email", "InternetEmailAddress", "email_address", "EmailAddress", "e_mail", "applicant_email", "candidate_email", "contact_email"]:
                            email = _xt(parent, tag_variant)
                            if email:
                                break
                        for tag_variant in ["phone", "Phone", "phone_number", "PhoneNumber", "FormattedNumber", "telephone", "contact_phone", "home_phone", "cell_phone", "mobile"]:
                            phone = _xt(parent, tag_variant)
                            if phone:
                                break
                        if first or last:
                            found.append(dict(first_name=first, last_name=last, email=email,
                                            phone=phone, accio_order_number="",
                                            accio_remote_number="", company_name="", account_name=""))
            return found

        applicants = deep_scan_for_applicants(root)

    return applicants, None

def _xt(el, tag):
    c = el.find(tag)
    return c.text.strip() if c is not None and c.text else ""

# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------
def send_release_email(db, applicant_id):
    a = db.execute("SELECT * FROM applicants WHERE id = %s", (applicant_id,)).fetchone()
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

    # Create tracking token and insert tracking record
    tracking_token = str(uuid.uuid4())
    tracking_pixel = f'<img src="https://fingerprint-release-manager1.onrender.com/api/track/{tracking_token}" width="1" height="1" alt="" />'

    # Convert body to HTML with tracking pixel
    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; line-height: 1.6;">
    <pre style="white-space: pre-wrap; word-wrap: break-word;">{h(body)}</pre>
    {tracking_pixel}
    </body>
    </html>
    """

    msg.attach(MIMEText(html_body, "html"))

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
        cur = db.execute("INSERT INTO email_log (applicant_id,recipient_email,subject,status) VALUES (%s,%s,%s,'sent') RETURNING id",
                        (applicant_id, a["email"], subj))
        email_log_id = cur.fetchone()["id"]

        # Create tracking record
        db.execute(
            "INSERT INTO email_tracking (applicant_id, email_log_id, tracking_token) VALUES (%s, %s, %s)",
            (applicant_id, email_log_id, tracking_token)
        )

        db.execute("UPDATE applicants SET email_sent=1, email_sent_at=%s, status='emailed', updated_at=%s WHERE id = %s", (now, now, applicant_id))
        db.commit()
        return True, "Email sent"
    except Exception as e:
        db.execute("INSERT INTO email_log (applicant_id,recipient_email,subject,status,error_message) VALUES (%s,%s,%s,'failed',%s)",
                   (applicant_id, a["email"], subj, str(e)))
        db.commit()
        return False, str(e)

def assign_code(db, applicant_id):
    a = db.execute("SELECT * FROM applicants WHERE id = %s", (applicant_id,)).fetchone()
    if not a: return None, "Not found"
    if a["assigned_code"]: return a["assigned_code"], "Already assigned"
    code_row = db.execute("SELECT id, code FROM codes WHERE assigned_to IS NULL LIMIT 1").fetchone()
    if not code_row: return None, "No codes available"
    now = datetime.now().isoformat()
    db.execute("UPDATE codes SET assigned_to=%s, assigned_date=%s WHERE id = %s", (applicant_id, now, code_row["id"]))
    db.execute("UPDATE applicants SET assigned_code=%s, status='code_assigned', updated_at=%s WHERE id = %s", (code_row["code"], now, applicant_id))
    db.commit()
    return code_row["code"], "OK"

def import_codes_from_file(filepath, column_index=0, skip_header=True, batch_name=None):
    imported = 0
    duplicates = 0
    error_msg = None
    try:
        if filepath.endswith(".xlsx") and HAS_OPENPYXL:
            wb = openpyxl.load_workbook(filepath)
            ws = wb.active
            start_row = 2 if skip_header else 1
            for row_idx, row in enumerate(ws.iter_rows(min_row=start_row), start=start_row):
                cell = row[column_index] if column_index < len(row) else None
                if cell and cell.value:
                    code = str(cell.value).strip()
                    if code:
                        db = get_db()
                        try:
                            db.execute("INSERT INTO codes (code, batch_name) VALUES (%s, %s)", (code, batch_name or "Import"))
                            imported += 1
                        except psycopg2.IntegrityError:
                            duplicates += 1
                        finally:
                            db.close()
        else:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.reader(f)
                if skip_header:
                    next(reader, None)
                for row in reader:
                    if column_index < len(row):
                        code = row[column_index].strip()
                        if code:
                            db = get_db()
                            try:
                                db.execute("INSERT INTO codes (code, batch_name) VALUES (%s, %s)", (code, batch_name or "Import"))
                                imported += 1
                            except psycopg2.IntegrityError:
                                duplicates += 1
                            finally:
                                db.close()
    except Exception as e:
        error_msg = str(e)
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
                        imp, dup, err = import_codes_from_file(fpath, column_index=col, skip_header=True, batch_name=f"Auto-Import {fname}")
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
# HTML Utilities
# ---------------------------------------------------------------------------
def h(text):
    """HTML escape."""
    return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;").replace("'", "&#39;")

def fmt_dt(val):
    """Format datetime for display."""
    if not val: return "-"
    if isinstance(val, str):
        try:
            val = datetime.fromisoformat(val)
        except Exception:
            return val
    return val.strftime("%Y-%m-%d %H:%M")

def render_page(title, content, active=""):
    """Render a full page with navigation."""
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{h(title)} - Fingerprint Release Manager</title>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
        <style>
            :root {{
                --primary: #2563eb;
                --primary-dark: #1e40af;
                --success: #10b981;
                --danger: #ef4444;
                --warning: #f59e0b;
                --gray-50: #f9fafb;
                --gray-100: #f3f4f6;
                --gray-200: #e5e7eb;
                --gray-300: #d1d5db;
                --gray-400: #9ca3af;
                --gray-500: #6b7280;
                --gray-700: #374151;
                --gray-900: #111827;
            }}
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}
            body {{
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
                background: var(--gray-50);
                color: var(--gray-900);
            }}
            .container {{
                display: flex;
                min-height: 100vh;
            }}
            .sidebar {{
                width: 250px;
                background: var(--gray-900);
                color: white;
                padding: 2rem 0;
                overflow-y: auto;
                box-shadow: 2px 0 8px rgba(0,0,0,0.1);
            }}
            .sidebar-brand {{
                padding: 0 1.5rem 2rem;
                font-size: 1.5rem;
                font-weight: bold;
                display: flex;
                align-items: center;
                gap: 0.5rem;
                border-bottom: 1px solid var(--gray-700);
            }}
            .sidebar-brand i {{
                color: var(--primary);
            }}
            .sidebar-nav {{
                list-style: none;
                padding: 1rem 0;
            }}
            .sidebar-nav li {{
                margin: 0;
            }}
            .sidebar-nav a {{
                display: flex;
                align-items: center;
                gap: 0.75rem;
                padding: 0.75rem 1.5rem;
                color: var(--gray-300);
                text-decoration: none;
                transition: all 0.2s;
            }}
            .sidebar-nav a:hover {{
                color: white;
                background: rgba(37, 99, 235, 0.1);
                padding-left: 1.75rem;
            }}
            .sidebar-nav a.active {{
                color: var(--primary);
                background: rgba(37, 99, 235, 0.1);
                border-left: 3px solid var(--primary);
                padding-left: 1.5rem;
            }}
            .main {{
                flex: 1;
                display: flex;
                flex-direction: column;
            }}
            .topbar {{
                background: white;
                padding: 1rem 2rem;
                border-bottom: 1px solid var(--gray-200);
                display: flex;
                justify-content: space-between;
                align-items: center;
                box-shadow: 0 1px 3px rgba(0,0,0,0.05);
            }}
            .topbar-user {{
                display: flex;
                align-items: center;
                gap: 1rem;
            }}
            .topbar-user a {{
                color: var(--primary);
                text-decoration: none;
                font-size: 0.875rem;
            }}
            .topbar-user a:hover {{
                text-decoration: underline;
            }}
            .content {{
                flex: 1;
                overflow-y: auto;
                padding: 2rem;
            }}
            .page-title {{
                font-size: 2rem;
                font-weight: bold;
                margin-bottom: 1.5rem;
                color: var(--gray-900);
            }}
            .alert {{
                padding: 1rem;
                border-radius: 0.5rem;
                margin-bottom: 1rem;
                display: flex;
                gap: 0.75rem;
                align-items: flex-start;
            }}
            .alert-success {{
                background: rgba(16, 185, 129, 0.1);
                border: 1px solid var(--success);
                color: var(--success);
            }}
            .alert-error {{
                background: rgba(239, 68, 68, 0.1);
                border: 1px solid var(--danger);
                color: var(--danger);
            }}
            .card {{
                background: white;
                border-radius: 0.5rem;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                padding: 1.5rem;
                margin-bottom: 1.5rem;
            }}
            .card-title {{
                font-size: 1.25rem;
                font-weight: 600;
                margin-bottom: 1rem;
                display: flex;
                align-items: center;
                gap: 0.5rem;
            }}
            .card-title i {{
                color: var(--primary);
            }}
            .stats {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 1rem;
                margin-bottom: 2rem;
            }}
            .stat-card {{
                background: white;
                border-radius: 0.5rem;
                padding: 1.5rem;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                text-align: center;
            }}
            .stat-value {{
                font-size: 2rem;
                font-weight: bold;
                color: var(--primary);
                margin: 0.5rem 0;
            }}
            .stat-label {{
                color: var(--gray-500);
                font-size: 0.875rem;
            }}
            .stat-icon {{
                font-size: 2rem;
                color: var(--primary);
                margin-bottom: 0.5rem;
                opacity: 0.7;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 1rem;
            }}
            thead {{
                background: var(--gray-100);
                border-bottom: 2px solid var(--gray-200);
            }}
            th {{
                padding: 0.75rem;
                text-align: left;
                font-weight: 600;
                color: var(--gray-700);
                font-size: 0.875rem;
            }}
            td {{
                padding: 0.75rem;
                border-bottom: 1px solid var(--gray-200);
            }}
            tbody tr:hover {{
                background: var(--gray-50);
            }}
            .btn {{
                padding: 0.5rem 1rem;
                border: none;
                border-radius: 0.375rem;
                font-size: 0.875rem;
                font-weight: 500;
                cursor: pointer;
                text-decoration: none;
                display: inline-flex;
                align-items: center;
                gap: 0.5rem;
                transition: all 0.2s;
            }}
            .btn-primary {{
                background: var(--primary);
                color: white;
            }}
            .btn-primary:hover {{
                background: var(--primary-dark);
            }}
            .btn-success {{
                background: var(--success);
                color: white;
            }}
            .btn-success:hover {{
                background: #059669;
            }}
            .btn-danger {{
                background: var(--danger);
                color: white;
            }}
            .btn-danger:hover {{
                background: #dc2626;
            }}
            .btn-small {{
                padding: 0.25rem 0.75rem;
                font-size: 0.75rem;
            }}
            .form-group {{
                margin-bottom: 1.5rem;
            }}
            label {{
                display: block;
                margin-bottom: 0.5rem;
                font-weight: 500;
                color: var(--gray-700);
            }}
            input[type="text"],
            input[type="email"],
            input[type="password"],
            input[type="number"],
            select,
            textarea {{
                width: 100%;
                padding: 0.5rem;
                border: 1px solid var(--gray-300);
                border-radius: 0.375rem;
                font-size: 0.875rem;
                font-family: inherit;
            }}
            input:focus,
            select:focus,
            textarea:focus {{
                outline: none;
                border-color: var(--primary);
                box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.1);
            }}
            .status-badge {{
                display: inline-block;
                padding: 0.25rem 0.75rem;
                border-radius: 1rem;
                font-size: 0.75rem;
                font-weight: 600;
                text-transform: uppercase;
            }}
            .status-pending {{
                background: rgba(239, 68, 68, 0.1);
                color: var(--danger);
            }}
            .status-code_assigned {{
                background: rgba(245, 158, 11, 0.1);
                color: var(--warning);
            }}
            .status-emailed {{
                background: rgba(59, 130, 246, 0.1);
                color: var(--primary);
            }}
            .status-opened {{
                background: rgba(16, 185, 129, 0.1);
                color: var(--success);
            }}
            .status-completed {{
                background: rgba(16, 185, 129, 0.1);
                color: var(--success);
            }}
            .email-status {{
                display: inline-block;
                width: 12px;
                height: 12px;
                border-radius: 50%;
                margin-right: 0.25rem;
            }}
            .email-status-opened {{
                background: var(--success);
            }}
            .email-status-not-opened {{
                background: var(--danger);
            }}
            .email-status-unsent {{
                background: var(--gray-300);
            }}
            .es {{
                text-align: center;
                padding: 3rem;
            }}
            .es i {{
                font-size: 4rem;
                color: var(--gray-300);
                margin-bottom: 1rem;
            }}
            .es h3 {{
                color: var(--gray-500);
            }}
            .grid-2 {{
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 1.5rem;
            }}
            @media (max-width: 768px) {{
                .container {{
                    flex-direction: column;
                }}
                .sidebar {{
                    width: 100%;
                    padding: 1rem 0;
                }}
                .sidebar-brand {{
                    padding: 1rem;
                }}
                .grid-2 {{
                    grid-template-columns: 1fr;
                }}
                .stats {{
                    grid-template-columns: 1fr;
                }}
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
                </nav>
            </div>
            <div class="main">
                <div class="topbar">
                    <div></div>
                    <div class="topbar-user">
                        <span><i class="fas fa-user"></i></span>
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

def flash(msg, cat="success"):
    """Store a flash message in session."""
    # For now, use simple in-memory storage (in production, store in session)
    pass

_flashes = {}

def render_flashes():
    """Render any pending flash messages."""
    global _flashes
    if not _flashes:
        return ""
    html = ""
    for cat, msg in _flashes.items():
        html += f'<div class="alert alert-{cat}"><i class="fas fa-check-circle"></i> {h(msg)}</div>'
    _flashes = {}
    return html

# Page renderers
def page_login():
    """Login page."""
    return f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Login - Fingerprint Release Manager</title>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
        <style>
            :root {{
                --primary: #2563eb;
                --gray-50: #f9fafb;
                --gray-200: #e5e7eb;
                --gray-400: #9ca3af;
                --gray-700: #374151;
                --gray-900: #111827;
            }}
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}
            body {{
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
                background: linear-gradient(135deg, var(--primary) 0%, #1e40af 100%);
                min-height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
            }}
            .login-card {{
                background: white;
                border-radius: 0.5rem;
                box-shadow: 0 10px 25px rgba(0,0,0,0.2);
                padding: 3rem;
                width: 100%;
                max-width: 400px;
            }}
            .login-brand {{
                text-align: center;
                margin-bottom: 2rem;
            }}
            .login-brand i {{
                font-size: 3rem;
                color: var(--primary);
                margin-bottom: 0.5rem;
            }}
            .login-brand h1 {{
                font-size: 1.5rem;
                color: var(--gray-900);
                margin: 0;
            }}
            .login-brand p {{
                color: var(--gray-400);
                margin: 0.5rem 0 0 0;
                font-size: 0.875rem;
            }}
            .form-group {{
                margin-bottom: 1.5rem;
            }}
            label {{
                display: block;
                margin-bottom: 0.5rem;
                font-weight: 500;
                color: var(--gray-700);
            }}
            input {{
                width: 100%;
                padding: 0.75rem;
                border: 1px solid var(--gray-200);
                border-radius: 0.375rem;
                font-size: 0.875rem;
                font-family: inherit;
            }}
            input:focus {{
                outline: none;
                border-color: var(--primary);
                box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.1);
            }}
            .btn {{
                width: 100%;
                padding: 0.75rem;
                background: var(--primary);
                color: white;
                border: none;
                border-radius: 0.375rem;
                font-size: 0.875rem;
                font-weight: 500;
                cursor: pointer;
                transition: background 0.2s;
            }}
            .btn:hover {{
                background: #1e40af;
            }}
            .alert {{
                background: rgba(239, 68, 68, 0.1);
                border: 1px solid #ef4444;
                color: #ef4444;
                padding: 0.75rem;
                border-radius: 0.375rem;
                margin-bottom: 1.5rem;
                font-size: 0.875rem;
            }}
        </style>
    </head>
    <body>
        <div class="login-card">
            <div class="login-brand">
                <i class="fas fa-fingerprint"></i>
                <h1>Fingerprint Release</h1>
                <p>Manager v2.0</p>
            </div>
            <form method="POST" action="/login">
                <div class="form-group">
                    <label for="username">Username</label>
                    <input type="text" id="username" name="username" required autofocus>
                </div>
                <div class="form-group">
                    <label for="password">Password</label>
                    <input type="password" id="password" name="password" required>
                </div>
                <button type="submit" class="btn">Sign In</button>
            </form>
        </div>
    </body>
    </html>
    """

def page_dashboard(db):
    """Dashboard with analytics."""
    total_app = db.execute("SELECT COUNT(*) as cnt FROM applicants").fetchone()["cnt"]
    pending = db.execute("SELECT COUNT(*) as cnt FROM applicants WHERE status='pending'").fetchone()["cnt"]
    emailed = db.execute("SELECT COUNT(*) as cnt FROM applicants WHERE status='emailed'").fetchone()["cnt"]
    codes_avail = db.execute("SELECT COUNT(*) as cnt FROM codes WHERE assigned_to IS NULL").fetchone()["cnt"]
    codes_used = db.execute("SELECT COUNT(*) as cnt FROM codes WHERE assigned_to IS NOT NULL").fetchone()["cnt"]

    # Recent activity
    activity = db.execute("""
        SELECT 'new_applicant' as type, id, first_name, last_name, created_at FROM applicants
        UNION ALL
        SELECT 'email_sent' as type, id, recipient_email, subject, sent_at FROM email_log
        ORDER BY COALESCE(created_at, sent_at) DESC
        LIMIT 10
    """).fetchall()

    # Clients
    clients = db.execute("""
        SELECT c.id, c.company_name, c.account_name, COUNT(a.id) as app_count
        FROM clients c
        LEFT JOIN applicants a ON a.client_id = c.id
        GROUP BY c.id, c.company_name, c.account_name
        ORDER BY app_count DESC
        LIMIT 5
    """).fetchall()

    stats_html = f"""
    <div class="stats">
        <div class="stat-card">
            <div class="stat-icon"><i class="fas fa-users"></i></div>
            <div class="stat-label">Total Applicants</div>
            <div class="stat-value">{total_app}</div>
        </div>
        <div class="stat-card">
            <div class="stat-icon"><i class="fas fa-clock"></i></div>
            <div class="stat-label">Pending</div>
            <div class="stat-value">{pending}</div>
        </div>
        <div class="stat-card">
            <div class="stat-icon"><i class="fas fa-envelope"></i></div>
            <div class="stat-label">Emailed</div>
            <div class="stat-value">{emailed}</div>
        </div>
        <div class="stat-card">
            <div class="stat-icon"><i class="fas fa-check"></i></div>
            <div class="stat-label">Codes Available</div>
            <div class="stat-value">{codes_avail}</div>
        </div>
        <div class="stat-card">
            <div class="stat-icon"><i class="fas fa-lock"></i></div>
            <div class="stat-label">Codes Used</div>
            <div class="stat-value">{codes_used}</div>
        </div>
    </div>
    """

    clients_html = """
    <div class="card">
        <div class="card-title">
            <i class="fas fa-building"></i> Top Clients
        </div>
        <table>
            <thead>
                <tr>
                    <th>Company</th>
                    <th>Account</th>
                    <th>Applicants</th>
                </tr>
            </thead>
            <tbody>
    """
    for c in clients:
        clients_html += f"""
                <tr>
                    <td><a href="/clients/{c['id']}" style="color: var(--primary); text-decoration: none;">{h(c['company_name'])}</a></td>
                    <td>{h(c['account_name'] or '-')}</td>
                    <td>{c['app_count']}</td>
                </tr>
        """
    clients_html += """
            </tbody>
        </table>
    </div>
    """

    activity_html = """
    <div class="card">
        <div class="card-title">
            <i class="fas fa-history"></i> Recent Activity
        </div>
        <table>
            <thead>
                <tr>
                    <th>Type</th>
                    <th>Details</th>
                    <th>Time</th>
                </tr>
            </thead>
            <tbody>
    """
    for a in activity:
        if a["type"] == "new_applicant":
            activity_html += f"""
                <tr>
                    <td><span class="status-badge status-pending">New</span></td>
                    <td>{h(a['first_name'])} {h(a['last_name'])}</td>
                    <td>{fmt_dt(a['created_at'])}</td>
                </tr>
            """
        elif a["type"] == "email_sent":
            activity_html += f"""
                <tr>
                    <td><span class="status-badge status-emailed">Email</span></td>
                    <td>{h(a['recipient_email'] or '-')} - {h(a['subject'] or '-')}</td>
                    <td>{fmt_dt(a['sent_at'])}</td>
                </tr>
            """
    activity_html += """
            </tbody>
        </table>
    </div>
    """

    return render_page("Dashboard", stats_html + clients_html + activity_html, active="dashboard")

def page_applicants(db, params):
    """List and manage applicants."""
    search = (params.get("search", [None])[0] or "").lower()
    rows = db.execute("SELECT * FROM applicants ORDER BY created_at DESC").fetchall()
    if search:
        rows = [r for r in rows if search in f"{r['first_name']} {r['last_name']}".lower()]

    content = f"""
    <div style="margin-bottom: 1rem; display: flex; gap: 0.5rem;">
        <form method="GET" style="flex: 1; display: flex; gap: 0.5rem;">
            <input type="text" name="search" placeholder="Search by name..." style="flex: 1;" value="{h(search)}">
            <button type="submit" class="btn btn-primary"><i class="fas fa-search"></i> Search</button>
        </form>
        <a href="/applicants/add" class="btn btn-primary"><i class="fas fa-plus"></i> Add Applicant</a>
        <form method="POST" action="/applicants/bulk-process" style="margin: 0;">
            <button type="submit" class="btn btn-success"><i class="fas fa-rocket"></i> Bulk Process Pending</button>
        </form>
    </div>
    <div class="card">
        <table>
            <thead>
                <tr>
                    <th>Status</th>
                    <th>Name</th>
                    <th>Email</th>
                    <th>Code</th>
                    <th>Email Opened</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
    """

    for r in rows:
        # Check if email was opened
        email_status = ""
        if not r["email_sent"]:
            email_status = '<span class="email-status email-status-unsent"></span> Not Sent'
        else:
            opened = db.execute(
                "SELECT COUNT(*) as cnt FROM email_tracking WHERE applicant_id = %s AND opened_at IS NOT NULL",
                (r["id"],)
            ).fetchone()["cnt"] > 0
            if opened:
                email_status = '<span class="email-status email-status-opened"></span> Yes'
            else:
                email_status = '<span class="email-status email-status-not-opened"></span> No'

        content += f"""
                <tr>
                    <td><span class="status-badge status-{r['status']}">{h(r['status'])}</span></td>
                    <td>{h(r['first_name'])} {h(r['last_name'])}</td>
                    <td>{h(r['email'] or '-')}</td>
                    <td><code>{h(r['assigned_code'] or '-')}</code></td>
                    <td>{email_status}</td>
                    <td style="white-space: nowrap;">
                        <a href="/applicants/{r['id']}/assign-and-send" class="btn btn-primary btn-small"><i class="fas fa-envelope"></i> Assign & Send</a>
                        <a href="/applicants/{r['id']}/delete" class="btn btn-danger btn-small" onclick="return confirm('Delete?')"><i class="fas fa-trash"></i></a>
                    </td>
                </tr>
        """

    content += """
            </tbody>
        </table>
    </div>
    """
    return render_page("Applicants", content, active="applicants")

def page_add_applicant():
    """Add applicant form."""
    content = """
    <div class="card">
        <form method="POST" action="/applicants/add">
            <div class="grid-2">
                <div class="form-group">
                    <label for="first_name">First Name</label>
                    <input type="text" id="first_name" name="first_name" required>
                </div>
                <div class="form-group">
                    <label for="last_name">Last Name</label>
                    <input type="text" id="last_name" name="last_name" required>
                </div>
            </div>
            <div class="grid-2">
                <div class="form-group">
                    <label for="email">Email</label>
                    <input type="email" id="email" name="email">
                </div>
                <div class="form-group">
                    <label for="phone">Phone</label>
                    <input type="text" id="phone" name="phone">
                </div>
            </div>
            <div class="form-group">
                <label for="accio_order_number">Accio Order Number</label>
                <input type="text" id="accio_order_number" name="accio_order_number">
            </div>
            <div class="form-group">
                <label for="notes">Notes</label>
                <textarea id="notes" name="notes" style="min-height: 100px;"></textarea>
            </div>
            <button type="submit" class="btn btn-primary"><i class="fas fa-save"></i> Add Applicant</button>
            <a href="/applicants" class="btn" style="background: var(--gray-300); color: var(--gray-900);">Cancel</a>
        </form>
    </div>
    """
    return render_page("Add Applicant", content, active="applicants")

def page_codes(db, params):
    """Manage payment codes."""
    avail = db.execute("SELECT COUNT(*) as cnt FROM codes WHERE assigned_to IS NULL").fetchone()["cnt"]
    assigned = db.execute("SELECT COUNT(*) as cnt FROM codes WHERE assigned_to IS NOT NULL").fetchone()["cnt"]

    rows = db.execute("SELECT * FROM codes ORDER BY imported_at DESC LIMIT 100").fetchall()

    content = f"""
    <div style="margin-bottom: 1rem; display: flex; gap: 0.5rem;">
        <a href="/codes/import" class="btn btn-primary"><i class="fas fa-upload"></i> Import from File</a>
        <a href="/codes/manual" class="btn btn-primary"><i class="fas fa-plus"></i> Add Manually</a>
    </div>
    <div class="stats">
        <div class="stat-card">
            <div class="stat-icon"><i class="fas fa-check"></i></div>
            <div class="stat-label">Available</div>
            <div class="stat-value">{avail}</div>
        </div>
        <div class="stat-card">
            <div class="stat-icon"><i class="fas fa-lock"></i></div>
            <div class="stat-label">Assigned</div>
            <div class="stat-value">{assigned}</div>
        </div>
    </div>
    <div class="card">
        <table>
            <thead>
                <tr>
                    <th>Code</th>
                    <th>Status</th>
                    <th>Batch</th>
                    <th>Assigned To</th>
                    <th>Date</th>
                </tr>
            </thead>
            <tbody>
    """

    for r in rows[:50]:
        assigned_to = "-"
        if r["assigned_to"]:
            a = db.execute("SELECT first_name, last_name FROM applicants WHERE id = %s", (r["assigned_to"],)).fetchone()
            if a:
                assigned_to = f"{h(a['first_name'])} {h(a['last_name'])}"

        content += f"""
                <tr>
                    <td><code>{h(r['code'])}</code></td>
                    <td><span class="status-badge {'status-pending' if not r['assigned_to'] else 'status-code_assigned'}">{h(r['status'])}</span></td>
                    <td>{h(r['batch_name'] or '-')}</td>
                    <td>{assigned_to}</td>
                    <td>{fmt_dt(r['imported_at'])}</td>
                </tr>
        """

    content += """
            </tbody>
        </table>
    </div>
    """
    return render_page("Payment Codes", content, active="codes")

def page_import_codes():
    """Import codes from file."""
    content = """
    <div class="card">
        <form method="POST" action="/codes/import" enctype="multipart/form-data">
            <div class="form-group">
                <label for="file">Excel or CSV File</label>
                <input type="file" id="file" name="file" accept=".xlsx,.csv" required>
            </div>
            <div class="form-group">
                <label for="column_index">Column Number (0-indexed)</label>
                <input type="number" id="column_index" name="column_index" value="0" min="0">
            </div>
            <div class="form-group">
                <label>
                    <input type="checkbox" name="skip_header" checked>
                    Skip first row (header)
                </label>
            </div>
            <div class="form-group">
                <label for="batch_name">Batch Name</label>
                <input type="text" id="batch_name" name="batch_name" placeholder="e.g., 'January 2024 Import'">
            </div>
            <button type="submit" class="btn btn-primary"><i class="fas fa-upload"></i> Import Codes</button>
            <a href="/codes" class="btn" style="background: var(--gray-300); color: var(--gray-900);">Cancel</a>
        </form>
    </div>
    """
    return render_page("Import Payment Codes", content, active="codes")

def page_settings(db):
    """Settings page."""
    content = """
    <div class="card">
        <div class="card-title">
            <i class="fas fa-cog"></i> SMTP Configuration
        </div>
        <form method="POST" action="/settings">
            <div class="grid-2">
                <div class="form-group">
                    <label for="smtp_server">SMTP Server</label>
                    <input type="text" id="smtp_server" name="smtp_server" value="{}" required>
                </div>
                <div class="form-group">
                    <label for="smtp_port">SMTP Port</label>
                    <input type="number" id="smtp_port" name="smtp_port" value="{}" required>
                </div>
            </div>
            <div class="grid-2">
                <div class="form-group">
                    <label for="smtp_username">SMTP Username</label>
                    <input type="text" id="smtp_username" name="smtp_username" value="{}">
                </div>
                <div class="form-group">
                    <label for="smtp_password">SMTP Password</label>
                    <input type="password" id="smtp_password" name="smtp_password" value="{}">
                </div>
            </div>
            <div class="form-group">
                <label>
                    <input type="hidden" name="smtp_use_tls" value="0">
                    <input type="checkbox" name="smtp_use_tls" value="1">
                    Use TLS
                </label>
            </div>
            <div class="grid-2">
                <div class="form-group">
                    <label for="sender_email">Sender Email</label>
                    <input type="email" id="sender_email" name="sender_email" value="{}" required>
                </div>
                <div class="form-group">
                    <label for="sender_name">Sender Name</label>
                    <input type="text" id="sender_name" name="sender_name" value="{}">
                </div>
            </div>
            <div class="form-group">
                <label for="email_subject">Email Subject Template</label>
                <input type="text" id="email_subject" name="email_subject" value="{}">
            </div>
            <div class="form-group">
                <label for="email_body">Email Body Template</label>
                <textarea id="email_body" name="email_body" style="min-height: 300px;">{}</textarea>
                <p style="margin-top: 0.5rem; color: var(--gray-500); font-size: 0.875rem;">
                    Available placeholders: {{first_name}}, {{last_name}}, {{code}}, {{company_name}}, {{ori_number}}
                </p>
            </div>
            <button type="submit" class="btn btn-primary"><i class="fas fa-save"></i> Save Settings</button>
        </form>
    </div>
    """.format(
        h(get_setting(db, "smtp_server")),
        h(get_setting(db, "smtp_port")),
        h(get_setting(db, "smtp_username")),
        h(get_setting(db, "smtp_password")),
        h(get_setting(db, "sender_email")),
        h(get_setting(db, "sender_name")),
        h(get_setting(db, "email_subject")),
        h(get_setting(db, "email_body"))
    )
    return render_page("Settings", content, active="settings")

def page_logs(db):
    """View XML logs."""
    rows = db.execute("SELECT * FROM xml_log ORDER BY id DESC LIMIT 50").fetchall()

    content = """
    <div class="card">
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Direction</th>
                    <th>Status</th>
                    <th>Error</th>
                    <th>Received</th>
                </tr>
            </thead>
            <tbody>
    """

    for r in rows:
        content += f"""
                <tr>
                    <td>{r['id']}</td>
                    <td>{h(r['direction'] or '-')}</td>
                    <td>{h(r['parsed_status'] or '-')}</td>
                    <td>{h(r['error_message'][:50] if r['error_message'] else '-')}</td>
                    <td>{fmt_dt(r['received_at'])}</td>
                </tr>
        """

    content += """
            </tbody>
        </table>
    </div>
    """
    return render_page("Logs", content, active="logs")

def page_clients(db, params):
    """Clients page."""
    client_id = params.get("client_id", [None])[0]

    if client_id:
        # Show applicants for specific client
        client = db.execute("SELECT * FROM clients WHERE id = %s", (int(client_id),)).fetchone()
        if not client:
            return render_page("Not Found", '<div class="es"><i class="fas fa-exclamation-triangle"></i><h3>Client not found</h3></div>', active="clients")

        applicants = db.execute("SELECT * FROM applicants WHERE client_id = %s ORDER BY created_at DESC", (int(client_id),)).fetchall()

        content = f"""
        <a href="/clients" class="btn" style="background: var(--gray-300); color: var(--gray-900); margin-bottom: 1rem;"><i class="fas fa-arrow-left"></i> Back</a>
        <div class="card">
            <div class="card-title">{h(client['company_name'])}</div>
            <p><strong>Account:</strong> {h(client['account_name'] or '-')}</p>
            <p><strong>Email:</strong> {h(client['contact_email'] or '-')}</p>
            <p><strong>Phone:</strong> {h(client['contact_phone'] or '-')}</p>
            <p><strong>Total Applicants:</strong> {len(applicants)}</p>
        </div>
        <div class="card">
            <div class="card-title">Applicants</div>
            <table>
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Email</th>
                        <th>Status</th>
                        <th>Code</th>
                    </tr>
                </thead>
                <tbody>
        """

        for a in applicants:
            content += f"""
                    <tr>
                        <td>{h(a['first_name'])} {h(a['last_name'])}</td>
                        <td>{h(a['email'] or '-')}</td>
                        <td><span class="status-badge status-{a['status']}">{h(a['status'])}</span></td>
                        <td><code>{h(a['assigned_code'] or '-')}</code></td>
                    </tr>
            """

        content += """
                </tbody>
            </table>
        </div>
        """
    else:
        # Show all clients
        clients = db.execute("""
            SELECT c.*, COUNT(a.id) as app_count, MAX(a.created_at) as last_order
            FROM clients c
            LEFT JOIN applicants a ON a.client_id = c.id
            GROUP BY c.id
            ORDER BY app_count DESC
        """).fetchall()

        content = """
        <div class="card">
            <table>
                <thead>
                    <tr>
                        <th>Company</th>
                        <th>Account</th>
                        <th>Contact Email</th>
                        <th>Total Applicants</th>
                        <th>Last Order</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
        """

        for c in clients:
            content += f"""
                    <tr>
                        <td>{h(c['company_name'])}</td>
                        <td>{h(c['account_name'] or '-')}</td>
                        <td>{h(c['contact_email'] or '-')}</td>
                        <td>{c['app_count']}</td>
                        <td>{fmt_dt(c['last_order'])}</td>
                        <td><a href="/clients?client_id={c['id']}" class="btn btn-primary btn-small">View</a></td>
                    </tr>
            """

        content += """
                </tbody>
            </table>
        </div>
        """

    return render_page("Clients", content, active="clients")

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

    def _redirect(self, url, set_cookie=None):
        self.send_response(303)
        self.send_header("Location", url)
        if set_cookie:
            self.send_header("Set-Cookie", f"session_token={set_cookie}; Path=/; HttpOnly; Max-Age=86400")
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

    def _check_auth(self):
        """Check if user is authenticated, redirect to login if not."""
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

        # Check auth for all pages except /login and /api/accio-push and /api/track
        if not path.startswith("/api/") and path != "/login":
            user = self._check_auth()
            if not user:
                self._redirect("/login")
                return

        db = get_db()

        try:
            if path == "/login":
                self._send(200, page_login())
            elif path == "/logout":
                self._send(200, page_login())
                self._redirect("/login")
            elif path == "/":
                self._send(200, page_dashboard(db))
            elif path == "/applicants":
                self._send(200, page_applicants(db, params))
            elif path == "/applicants/add":
                self._send(200, page_add_applicant())
            elif path == "/clients":
                self._send(200, page_clients(db, params))
            elif path == "/codes":
                self._send(200, page_codes(db, params))
            elif path == "/codes/import":
                self._send(200, page_import_codes())
            elif path == "/settings":
                self._send(200, page_settings(db))
            elif path == "/logs":
                self._send(200, page_logs(db))
            elif path.startswith("/api/track/"):
                # Email tracking pixel endpoint
                token = path.split("/api/track/")[1]
                try:
                    db.execute(
                        "UPDATE email_tracking SET opened_at=NOW(), open_count=open_count+1, user_agent=%s WHERE tracking_token=%s",
                        (self.headers.get("User-Agent", ""), token)
                    )
                    # Update applicant status to opened
                    tracking = db.execute(
                        "SELECT applicant_id FROM email_tracking WHERE tracking_token=%s",
                        (token,)
                    ).fetchone()
                    if tracking:
                        db.execute(
                            "UPDATE applicants SET status='opened' WHERE id=%s",
                            (tracking["applicant_id"],)
                        )
                except Exception as e:
                    logger.error(f"Tracking error: {e}")

                # Return 1x1 transparent GIF
                gif = b'\x47\x49\x46\x38\x39\x61\x01\x00\x01\x00\x80\x00\x00\xff\xff\xff\x00\x00\x00\x21\xf9\x04\x01\x0a\x00\x01\x00\x2c\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02\x4d\x01\x00\x3b'
                self.send_response(200)
                self.send_header("Content-Type", "image/gif")
                self.send_header("Content-Length", str(len(gif)))
                self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
                self.end_headers()
                self.wfile.write(gif)
            elif path == "/api/debug-xml":
                # Return the full raw XML from the most recent xml_log entry
                row = db.execute("SELECT raw_xml FROM xml_log ORDER BY id DESC LIMIT 1").fetchone()
                if row and row["raw_xml"]:
                    self._send(200, row["raw_xml"], "text/xml")
                else:
                    self._send(200, "No XML logs found", "text/plain")
                return
            elif path == "/api/debug-xml-tags":
                # Return all unique XML tag names from most recent log
                row = db.execute("SELECT raw_xml FROM xml_log ORDER BY id DESC LIMIT 1").fetchone()
                if row and row["raw_xml"]:
                    try:
                        xroot = ET.fromstring(row["raw_xml"])
                        tags = set()
                        tag_tree = []
                        for el in xroot.iter():
                            tags.add(el.tag)
                            depth = 0
                            parent = el
                            text_preview = (el.text or "").strip()[:50]
                            tag_tree.append(f"{el.tag} = '{text_preview}'" if text_preview else el.tag)
                        result = "=== ALL UNIQUE TAGS ===\n" + "\n".join(sorted(tags))
                        result += "\n\n=== FULL TAG TREE WITH VALUES ===\n" + "\n".join(tag_tree)
                        self._send(200, result, "text/plain")
                    except Exception as e:
                        self._send(200, f"Parse error: {e}", "text/plain")
                else:
                    self._send(200, "No XML logs found", "text/plain")
                return
            else:
                self._send(404, render_page("Not Found", '<div class="es"><i class="fas fa-exclamation-triangle"></i><h3>Page not found</h3></div>'))
        finally:
            db.close()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        db = get_db()

        try:
            # Login endpoint
            if path == "/login":
                form_data = self._parse_form()
                def fv(name, default=""):
                    if isinstance(form_data, cgi.FieldStorage):
                        item = form_data.getfirst(name, default)
                        return item if isinstance(item, str) else item.decode() if item else default
                    else:
                        vals = form_data.get(name, [default])
                        return vals[0] if vals else default

                username = fv("username")
                password = fv("password")

                # Find user
                user = db.execute("SELECT * FROM users WHERE username=%s AND is_active=TRUE", (username,)).fetchone()
                if user and verify_password(password, user["password_hash"]):
                    # Valid login
                    token = create_session(db, user["id"])
                    db.execute("UPDATE users SET last_login=NOW() WHERE id=%s", (user["id"],))
                    self._redirect("/", set_cookie=token)
                else:
                    # Invalid login
                    self._send(200, page_login())
                return

            # Accio Data XML push endpoint (NO session auth required)
            if path == "/api/accio-push":
              try:
                ACCIO_USERNAME = os.environ.get("ACCIO_USERNAME", "admin")
                ACCIO_PASSWORD = os.environ.get("ACCIO_PASSWORD", "Fingerprint")
                # Read the body first (Accio may embed credentials in XML)
                length = int(self.headers.get("Content-Length", 0))
                raw = self.rfile.read(length).decode() if length > 0 else ""
                auth_valid = False
                # Method 1: HTTP Basic Auth header
                auth_header = self.headers.get("Authorization", "")
                if auth_header.startswith("Basic "):
                    try:
                        decoded = base64.b64decode(auth_header[6:]).decode()
                        u, p = decoded.split(":", 1)
                        if u == ACCIO_USERNAME and p == ACCIO_PASSWORD:
                            auth_valid = True
                    except Exception:
                        pass
                # Method 2: Query params
                qs = urllib.parse.parse_qs(parsed.query)
                if not auth_valid and qs.get("username", [None])[0] == ACCIO_USERNAME and qs.get("password", [None])[0] == ACCIO_PASSWORD:
                    auth_valid = True
                # Method 3: Credentials in the XML body (Accio XML format)
                if not auth_valid and raw.strip():
                    try:
                        auth_root = ET.fromstring(raw)
                        # First check for explicit <login> element (AccioOrder format)
                        login_elem = auth_root.find("login")
                        if login_elem is not None:
                            xml_user = (login_elem.findtext("username") or "").strip()
                            xml_pass = (login_elem.findtext("password") or "").strip()
                            if xml_user == ACCIO_USERNAME and xml_pass == ACCIO_PASSWORD:
                                auth_valid = True
                        # Check for account/username/password elements at various levels
                        if not auth_valid:
                            for parent in [auth_root] + list(auth_root):
                                xml_user = None
                                xml_pass = None
                                xml_acct = None
                                for el in parent.iter():
                                    tag = el.tag.lower() if el.tag else ""
                                    if tag in ("username", "user", "remote_username"):
                                        xml_user = (el.text or "").strip()
                                    elif tag in ("password", "pass", "remote_password"):
                                        xml_pass = (el.text or "").strip()
                                    elif tag in ("account", "remote_account", "acctid"):
                                        xml_acct = (el.text or "").strip()
                                if xml_user == ACCIO_USERNAME and xml_pass == ACCIO_PASSWORD:
                                    auth_valid = True
                                    break
                        # Also check root element attributes
                        if not auth_valid:
                            root_user = auth_root.get("username") or auth_root.get("user") or ""
                            root_pass = auth_root.get("password") or auth_root.get("pass") or ""
                            if root_user == ACCIO_USERNAME and root_pass == ACCIO_PASSWORD:
                                auth_valid = True
                    except ET.ParseError:
                        logging.warning("XML parse error during auth check")
                    except Exception as e:
                        logging.warning(f"Unexpected error during XML auth check: {e}")
                # Method 4: Check HTTP headers (some systems send custom headers)
                if not auth_valid:
                    h_user = self.headers.get("X-Username") or self.headers.get("Username") or ""
                    h_pass = self.headers.get("X-Password") or self.headers.get("Password") or ""
                    if h_user == ACCIO_USERNAME and h_pass == ACCIO_PASSWORD:
                        auth_valid = True
                # Log the auth attempt for debugging
                logging.info(f"Accio push auth: valid={auth_valid}, has_basic={bool(auth_header)}, has_body={len(raw)>0}, body_len={len(raw)}")
                if not auth_valid:
                    # Log the failed attempt with headers for debugging
                    all_headers = {k: v for k, v in self.headers.items() if k.lower() != "authorization"}
                    logging.warning(f"Auth failed. Headers: {all_headers}")
                    logging.warning(f"Auth failed. Body preview: {raw[:500]}")
                    db.execute("INSERT INTO xml_log (direction,raw_xml,parsed_status,error_message) VALUES ('inbound',%s,'auth_failed','Authentication failed - check vendor credentials')", (raw[:10000],))
                    db.commit()
                    self._send(401, '<?xml version="1.0" encoding="UTF-8"?>\n<BackgroundReports><error>Authentication required</error></BackgroundReports>', "text/xml")
                    return
                # Auth passed - log and process
                logging.info(f"Accio push auth PASSED. Processing XML ({len(raw)} bytes)...")
                # DEBUG: Log all XML tag names and their text values to help identify email/phone fields
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
                db.execute("INSERT INTO xml_log (direction,raw_xml,parsed_status) VALUES ('inbound',%s,'processing')", (raw[:10000],))
                db.commit()
                applicants_data, err = parse_accio_xml(raw)
                if err:
                    logging.error(f"XML parse error: {err}")
                    db.execute("UPDATE xml_log SET parsed_status='error',error_message=%s WHERE id=(SELECT MAX(id) FROM xml_log)", (err,))
                    db.commit()
                    self._send(400, '<?xml version="1.0" encoding="UTF-8"?>\n<BackgroundReports><error>XML parse error</error></BackgroundReports>', "text/xml")
                    return
                added = 0
                auto_assign = get_setting(db, "auto_assign_codes") == "1"
                auto_email = get_setting(db, "auto_send_email") == "1"
                for a in applicants_data:
                    try:
                        ex = db.execute("SELECT id FROM applicants WHERE accio_order_number = %s", (a["accio_order_number"],)).fetchone() if a["accio_order_number"] else None
                        if not ex:
                            # Auto-create or find client
                            client_id = None
                            if a.get("company_name"):
                                client = db.execute("SELECT id FROM clients WHERE company_name=%s", (a["company_name"],)).fetchone()
                                if not client:
                                    cur = db.execute(
                                        "INSERT INTO clients (company_name, account_name) VALUES (%s, %s) RETURNING id",
                                        (a["company_name"], a.get("account_name", ""))
                                    )
                                    client_id = cur.fetchone()["id"]
                                else:
                                    client_id = client["id"]

                            cur = db.execute("INSERT INTO applicants (first_name,last_name,email,phone,accio_order_number,accio_remote_number,client_id) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                                       (a["first_name"],a["last_name"],a["email"],a["phone"],a["accio_order_number"],a["accio_remote_number"],client_id))
                            new_id = cur.fetchone()["id"]
                            added += 1
                            # Auto-assign a payment code if enabled
                            if auto_assign:
                                code_row = db.execute("SELECT id, code FROM codes WHERE assigned_to IS NULL LIMIT 1").fetchone()
                                if code_row:
                                    db.execute("UPDATE codes SET assigned_to=%s, assigned_date=NOW() WHERE id = %s", (new_id, code_row["id"]))
                                    db.execute("UPDATE applicants SET assigned_code=%s, status='code_assigned' WHERE id = %s", (code_row["code"], new_id))
                                    db.commit()
                                    # Auto-send email if enabled
                                    if auto_email and a["email"]:
                                        try:
                                            send_release_email(db, new_id)
                                        except Exception as email_err:
                                            logging.error(f"Auto-email failed for applicant {new_id}: {email_err}")
                    except Exception as proc_err:
                        logging.error(f"Error processing applicant: {proc_err}")
                db.execute("UPDATE xml_log SET parsed_status='success',error_message=%s WHERE id=(SELECT MAX(id) FROM xml_log)", (f"Added {added} applicants from {len(applicants_data)} parsed",))
                db.commit()
                logging.info(f"Accio push complete: {added} added from {len(applicants_data)} parsed")
                # Respond with Accio-compatible XML acknowledgment
                resp_xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
                resp_xml += '<BackgroundReports>\n'
                resp_xml += '  <BackgroundReportPackage>\n'
                resp_xml += f'    <ScreeningStatus>accepted</ScreeningStatus>\n'
                resp_xml += f'    <ResultsRetrieved>{added}</ResultsRetrieved>\n'
                resp_xml += '  </BackgroundReportPackage>\n'
                resp_xml += '</BackgroundReports>'
                self._send(200, resp_xml, "text/xml")
                return
              except Exception as e:
                # Catch-all: ALWAYS send a valid response so Accio doesn't get "unable to read response"
                logging.error(f"CRITICAL: Unhandled exception in accio-push: {e}", exc_info=True)
                try:
                    db.execute("INSERT INTO xml_log (direction,raw_xml,parsed_status,error_message) VALUES ('inbound','','crash',%s)", (str(e)[:5000],))
                    db.commit()
                except Exception:
                    pass
                self._send(500, '<?xml version="1.0" encoding="UTF-8"?>\n<BackgroundReports><error>Internal server error</error></BackgroundReports>', "text/xml")
                return

            # Check auth for all other POST endpoints
            user = self._check_auth()
            if not user:
                self._redirect("/login")
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
                    for code in codes_list:
                        code = str(code).strip()
                        if code:
                            try:
                                db.execute("INSERT INTO codes (code, batch_name) VALUES (%s, %s)", (code, batch_name))
                                imported += 1
                            except psycopg2.IntegrityError:
                                dups += 1
                    self._send(200, json.dumps({"status": "success", "imported": imported, "duplicates": dups, "batch": batch_name}), "application/json")
                except json.JSONDecodeError:
                    self._send(400, json.dumps({"status": "error", "message": "Invalid JSON"}), "application/json")
                except Exception as e:
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
                db.execute("INSERT INTO applicants (first_name,last_name,email,phone,accio_order_number,notes) VALUES (%s,%s,%s,%s,%s,%s)",
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
                a = db.execute("SELECT * FROM applicants WHERE id = %s", (aid,)).fetchone()
                if a and not a["assigned_code"]:
                    assign_code(db, aid)
                ok, msg = send_release_email(db, aid)
                flash("Code assigned & email sent!" if ok else f"Code assigned but email failed: {msg}", "success" if ok else "error")
                self._redirect("/applicants")

            elif path.startswith("/applicants/") and path.endswith("/mark-complete"):
                aid = int(path.split("/")[2])
                db.execute("UPDATE applicants SET status='completed' WHERE id = %s", (aid,))
                db.commit()
                flash("Applicant marked complete.", "success")
                self._redirect("/applicants")

            elif path.startswith("/applicants/") and path.endswith("/delete"):
                aid = int(path.split("/")[2])
                # Free the assigned code back to the pool
                applicant = db.execute("SELECT assigned_code FROM applicants WHERE id = %s", (aid,)).fetchone()
                if applicant and applicant["assigned_code"]:
                    db.execute("UPDATE codes SET assigned_to=NULL, assigned_date=NULL WHERE code = %s", (applicant["assigned_code"],))
                db.execute("DELETE FROM applicants WHERE id = %s", (aid,))
                db.commit()
                flash("Applicant deleted.", "success")
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
                            db.execute("INSERT INTO codes (code,batch_name) VALUES (%s,%s)", (code_str, batch))
                            imp += 1
                        except psycopg2.IntegrityError:
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
                        # For checkboxes, getlist returns ["0","1"] when checked, ["0"] when unchecked
                        vals = form_data.getlist(key)
                        if vals:
                            set_setting(db, key, vals[-1])
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
                            # For checkboxes, take the LAST value (hidden="0" comes first, checkbox="1" comes second)
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
                        logger.info(f"Test email: From={sender}, To={addr}, User={srv_user}")
                        srv = smtplib.SMTP(srv_host, srv_port, timeout=15)
                        srv.set_debuglevel(1)
                        if use_tls:
                            srv.starttls()
                            logger.info("Test email: TLS established")
                        if srv_user and srv_pass:
                            srv.login(srv_user, srv_pass)
                            logger.info("Test email: Login successful")
                        srv.send_message(msg)
                        srv.quit()
                        logger.info(f"Test email: Sent successfully to {addr}")
                        flash(f"Test email sent to {addr}!", "success")
                    except Exception as e:
                        logger.error(f"Test email FAILED: {type(e).__name__}: {e}")
                        flash(f"Test failed: {type(e).__name__}: {e}", "error")
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
    ║   Fingerprint Release Manager v2.0                       ║
    ║   Web UI:      http://localhost:{PORT}                     ║
    ║                                                          ║
    ║   DEFAULT LOGIN:                                         ║
    ║   Username: admin                                        ║
    ║   Password: admin123                                     ║
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
    ║   GET  /api/track/{{token}} (Email tracking pixel)        ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    server = HTTPServer((HOST, PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        _watcher_running = False
        print("\nShutting down...")
        server.server_close()
