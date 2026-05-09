import io
import re
from datetime import datetime
from flask import Flask, request, send_file, jsonify
from flask_cors import CORS
import pandas as pd

app = Flask(__name__)
CORS(app, origins=["https://cleandata.cc", "https://www.cleandata.cc"])

# ── 1. BOŞ SATIR SİL ─────────────────────────────────────────────
def sil_bos_satirlar(df):
    # Sadece boşluk içeren hücreleri de boş say
    df2 = df.apply(lambda col: col.map(
        lambda x: None if isinstance(x, str) and x.strip() == "" else x
    ))
    return df2.dropna(how="all")
 
# ── 2. TEKRARLİ SATIR SİL ────────────────────────────────────────
def sil_tekrarli_satirlar(df):
    return df.drop_duplicates()
 
# ── 3. BOŞLUK TEMİZLE ────────────────────────────────────────────
def duzelt_bosluklar(df):
    return df.apply(lambda col: col.map(
        lambda x: x.strip() if isinstance(x, str) else x
    ))
 
# ── 4. HARF DÜZELTMEt ────────────────────────────────────────────
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
        "%Y-%m-%d %H:%M:%S", "%d.%m.%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ]
    def parse_et(x):
        # Timestamp veya datetime → direkt formatla
        if hasattr(x, "strftime"):
            return x.strftime(hedef_format)
        if not isinstance(x, str):
            return x
        s = x.strip()
        for fmt in [hedef_format] + yaygin:
            try:
                return datetime.strptime(s, fmt).strftime(hedef_format)
            except ValueError:
                continue
        return x
 
    def is_date_col(col):
        # Excel date tipindeki sütunları yakala
        import pandas.api.types as pat
        if pat.is_datetime64_any_dtype(col):
            return True
        kws = ["date","tarih","datum","fecha","dat","time","zaman"]
        if any(k in str(col.name).lower() for k in kws):
            return True
        # String hücreler içinde tarih benzeri değer var mı?
        sample = col.dropna().head(20)
        date_pat = re.compile(r'\d{1,4}[./-]\d{1,2}[./-]\d{2,4}')
        hits = sum(1 for v in sample if isinstance(v, str) and date_pat.search(v))
        return hits / max(len(sample), 1) > 0.4
 
    result = df.copy()
    for col in result.columns:
        if is_date_col(result[col]):
            result[col] = result[col].map(parse_et)
            # Sütunu string tipine zorla — Excel datetime olarak yazmasın
            result[col] = result[col].astype(str).replace("NaT", "")
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
 
        # 1. Tüm boşlukları kaldır (içerideki de dahil)
        c = re.sub(r'\s+', '', x).lower()
 
        if not c: return x
 
        # 2. @ var mı?
        if '@' not in c:
            return x  # @ yoksa dokunma
 
        # 3. @ sonrasında nokta yoksa bilinen domain ekle
        parts = c.split('@')
        if len(parts) == 2:
            user, domain = parts[0], parts[1]
            # domain boşsa dokunma
            if not domain:
                return x
            # domain'de nokta yoksa (örn: "gmail", "hotmail")
            if '.' not in domain:
                full_domain = COMMON_DOMAINS.get(domain, domain + ".com")
                c = f"{user}@{full_domain}"
 
        # 4. Geçerliyse döndür
        if valid_pat.match(c):
            return c
        # 5. Geçersiz ama @ var → temizlenmiş hali döndür
        return c
 
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
            df = pd.read_csv(dosya)
        else:
            # parse_dates=False → tarihleri string olarak oku, biz işleyeceğiz
            df = pd.read_excel(dosya, parse_dates=False)
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
 
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Cleaned Data")
        if warnings:
            pd.DataFrame({"Warnings": warnings}).to_excel(
                writer, index=False, sheet_name="Warnings")
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
 
@app.route("/")
def index():
    return "CleanData API is running."
 
if __name__ == "__main__":
    app.run(debug=True, port=5000)
