import io
import re
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from flask import Flask, request, send_file, jsonify
from flask_cors import CORS
import pandas as pd

app = Flask(__name__)
CORS(app)

@app.after_request
def after_request(response):
    response.headers.add("Access-Control-Allow-Origin", "https://cleandata.cc")
    response.headers.add("Access-Control-Allow-Headers", "Content-Type,Authorization")
    response.headers.add("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
    return response
    
# ── 1. BOŞ SATIR SİL ─────────────────────────────────────────────
def sil_bos_satirlar(df):
    def to_none(x):
        if x is None: return None
        if isinstance(x, float) and pd.isna(x): return None
        if isinstance(x, str) and x.strip() == "": return None
        return x
    df2 = df.apply(lambda col: col.map(to_none))
    return df2.dropna(how="all")

# ── 2. TEKRARLİ SATIR SİL ────────────────────────────────────────
def sil_tekrarli_satirlar(df):
    return df.drop_duplicates()

# ── 3. BOŞLUK TEMİZLE ────────────────────────────────────────────
def duzelt_bosluklar(df):
    return df.apply(lambda col: col.map(
        lambda x: x.strip() if isinstance(x, str) else x
    ))

# ── 4. HARF DÜZELT ───────────────────────────────────────────────
def duzelt_harf(df, mod="title"):
    def cevir(x):
        if not isinstance(x, str): return x
        return {"upper": x.upper, "lower": x.lower}.get(mod, x.title)()
    return df.apply(lambda col: col.map(cevir))

# ── 5. TARİH ─────────────────────────────────────────────────────
def duzelt_tarih(df, hedef_format="%d.%m.%Y"):
    yaygin = [
        "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y",
        "%Y/%m/%d", "%d.%m.%Y", "%Y.%m.%d",
    ]
    def parse_et(x):
        try:
            if pd.isna(x): return ""
        except Exception:
            pass
        if hasattr(x, "strftime"):
            return x.strftime(hedef_format)
        if not isinstance(x, str):
            return x
        s = x.strip().split(" ")[0].split("T")[0]
        for fmt in [hedef_format] + yaygin:
            try:
                return datetime.strptime(s, fmt).strftime(hedef_format)
            except ValueError:
                continue
        return x

    result = df.copy()
    for col in result.columns:
        import pandas.api.types as pat_types
        is_dt = pat_types.is_datetime64_any_dtype(result[col])
        kws = ["date","tarih","datum","fecha","zaman"]
        name_match = any(k in str(col).lower() for k in kws)
        if is_dt or name_match:
            result[col] = result[col].map(parse_et)
            result[col] = result[col].astype(str).replace("NaT", "").replace("nan", "")
    return result

def force_string_dates(df, hedef_format):
    date_pattern = re.compile(r"^\d{2}[./-]\d{2}[./-]\d{4}$")
    result = df.copy()
    for col in result.columns:
        sample = result[col].dropna().head(10)
        if any(isinstance(v, str) and date_pattern.match(v.strip()) for v in sample):
            result[col] = result[col].astype(str).replace("nan", "")
    return result

# ── 6. TELEFON ───────────────────────────────────────────────────
def normalize_telefon(df, default_cc="90"):
    def fmt_phone(x):
        if pd.isna(x): return x
        raw = str(int(x)) if isinstance(x, float) and x == int(x) else str(x)
        digits = re.sub(r'\D', '', raw)
        if len(digits) < 7: return x
        if len(digits) >= 11: cc, number = digits[:-10], digits[-10:]
        elif len(digits) == 10: cc, number = default_cc, digits
        else: return digits
        fmt = f"{number[:3]} {number[3:6]} {number[6:]}" if len(number) == 10 else number
        return f"+{cc} {fmt}"

    def is_phone_col(col):
        kws = ['phone','tel','mobile','gsm','cell','telefon','cep','numara','number']
        if any(k in str(col.name).lower() for k in kws): return True
        sample = col.dropna().head(20)
        pat = re.compile(r'^[\d\s\+\-\(\)\.]{7,20}$')
        hits = sum(1 for v in sample if isinstance(v, str) and pat.match(v.strip()))
        return hits / max(len(sample), 1) > 0.5

    result = df.copy()
    for col in result.columns:
        if is_phone_col(result[col]):
            result[col] = result[col].map(fmt_phone)
    return result

# ── 7. EMAIL ─────────────────────────────────────────────────────
COMMON_DOMAINS = {
    "gmail": "gmail.com", "hotmail": "hotmail.com", "yahoo": "yahoo.com",
    "outlook": "outlook.com", "icloud": "icloud.com", "yandex": "yandex.com",
    "protonmail": "protonmail.com",
}

def normalize_email(df):
    valid_pat = re.compile(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$')

    def fmt_email(x):
        if not isinstance(x, str): return x
        c = re.sub(r'\s+', '', x).lower()
        if not c or '@' not in c: return x
        parts = c.split('@')
        if len(parts) == 2:
            user, domain = parts[0], parts[1]
            if not domain: return x
            if '.' not in domain:
                c = f"{user}@{COMMON_DOMAINS.get(domain, domain + '.com')}"
        return c if valid_pat.match(c) else c

    def is_email_col(col):
        kws = ['email', 'e-mail', 'mail', 'eposta', 'e-posta']
        if any(k in str(col.name).lower() for k in kws): return True
        sample = col.dropna().head(20)
        hits = sum(1 for v in sample if isinstance(v, str) and '@' in v)
        return hits / max(len(sample), 1) > 0.4

    result = df.copy()
    for col in result.columns:
        if is_email_col(result[col]):
            result[col] = result[col].map(fmt_email)
    return result

# ── HESAPLANMIŞ ALAN TESPİTİ ─────────────────────────────────────
def tespit_hesaplanmis_alan(df):
    warnings = []
    kws = ['total','sum','toplam','tutar','amount','subtotal','grand','net',
           'gross','calculated','computed','formula','result','sonuc','hesap']
    for col in df.select_dtypes(include='number').columns:
        if any(k in str(col).lower() for k in kws):
            warnings.append(f"Column '{col}' may contain computed values — verify after cleaning.")
    return df, warnings

# ── MAİL GÖNDERİM (RESEND) ───────────────────────────────────────
def send_notification(sender_name, sender_email, description, file_bytes, filename):
    gmail_user   = os.environ.get("GMAIL_USER")
    gmail_pass   = os.environ.get("GMAIL_PASS")
    notify_email = os.environ.get("NOTIFY_EMAIL")

    if not gmail_user or not gmail_pass or not notify_email:
        return False, "Mail credentials not configured."

    try:
        msg = MIMEMultipart()
        msg["From"]     = f"CleanData <{gmail_user}>"
        msg["To"]       = notify_email
        msg["Reply-To"] = sender_email
        msg["Subject"]  = f"New Cleaning Request — {sender_name}"

        body = f"""New manual cleaning request:

Name: {sender_name}
Email: {sender_email}

Description:
{description}

File attached: {filename}

Reply to this email to respond directly to the user.
"""
        msg.attach(MIMEText(body, "plain"))

        part = MIMEBase("application", "octet-stream")
        part.set_payload(file_bytes)
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={filename}")
        msg.attach(part)

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(gmail_user, gmail_pass)
            server.sendmail(gmail_user, notify_email, msg.as_string())

        return True, "OK"
    except Exception as e:
        return False, str(e)

# ── ANA ENDPOINT ─────────────────────────────────────────────────
@app.route("/clean", methods=["POST"])
def temizle():
    if "file" not in request.files:
        return jsonify({"hata": "No file received."}), 400
    dosya = request.files["file"]
    if not dosya.filename.endswith((".xlsx", ".xls", ".csv")):
        return jsonify({"hata": "Only .xlsx, .xls, or .csv files are supported."}), 400

    islemler  = request.form.getlist("islemler")
    harf_modu = request.form.get("harf_modu", "title")
    tarih_fmt = request.form.get("tarih_format", "%d.%m.%Y")
    phone_cc  = request.form.get("phone_cc", "90")

    try:
        if dosya.filename.endswith(".csv"):
            df = pd.read_csv(dosya, engine="openpyxl")
        else:
            df = pd.read_excel(dosya, engine="openpyxl")
    except Exception as e:
        return jsonify({"hata": f"Could not read file: {str(e)}"}), 400

    original_rows = len(df)
    df, warnings = tespit_hesaplanmis_alan(df)

    if "bos_satir" in islemler: df = sil_bos_satirlar(df)
    if "tekrar"    in islemler: df = sil_tekrarli_satirlar(df)
    if "bosluk"    in islemler: df = duzelt_bosluklar(df)
    if "harf"      in islemler: df = duzelt_harf(df, mod=harf_modu)
    if "tarih"     in islemler: df = duzelt_tarih(df, hedef_format=tarih_fmt)
    if "telefon"   in islemler: df = normalize_telefon(df, default_cc=phone_cc)
    if "email"     in islemler: df = normalize_email(df)

    clean_rows = len(df)
    removed    = original_rows - clean_rows

    if "tarih" in islemler:
        df = force_string_dates(df, tarih_fmt)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl", date_format="DD.MM.YYYY", datetime_format="DD.MM.YYYY") as writer:
        df.to_excel(writer, index=False, sheet_name="Cleaned Data")
        if warnings:
            pd.DataFrame({"Warnings": warnings}).to_excel(writer, index=False, sheet_name="Warnings")
    output.seek(0)

    clean_name = re.sub(r'\.(xlsx?|csv)$', '_clean.xlsx', dosya.filename)
    resp = send_file(
        output,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=clean_name,
    )
    resp.headers["X-Silinen-Satir"]  = str(removed)
    resp.headers["X-Toplam-Satir"]   = str(clean_rows)
    resp.headers["X-Warnings-Count"] = str(len(warnings))
    return resp

# ── MANUEL TEMİZLEME ENDPOINT ────────────────────────────────────
@app.route("/manual-request", methods=["POST"])
def manuel_istek():
    name  = request.form.get("name", "").strip()
    email = request.form.get("email", "").strip()
    desc  = request.form.get("description", "").strip()

    if not email or not desc:
        return jsonify({"hata": "Email and description are required."}), 400

    if "file" not in request.files:
        return jsonify({"hata": "No file received."}), 400

    dosya = request.files["file"]
    file_bytes = dosya.read()

    success, msg = send_notification(name, email, desc, file_bytes, dosya.filename)

    if success:
        return jsonify({"mesaj": "Request received! We will contact you within 24 hours."}), 200
    else:
        return jsonify({"hata": f"Mail could not be sent: {msg}"}), 500'''
        
if old in content:
    content = content.replace(old, new)
    open('/mnt/user-data/outputs/app.py', 'w').write(content)
    print("done")
else:
    print("NOT FOUND")
EOF

@app.route("/")
def index():
    return "CleanData API is running."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

import gc
gc.collect()
